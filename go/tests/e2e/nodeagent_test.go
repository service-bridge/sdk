//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The Node side of a cross-language test runs as its own process: the two SDKs
// have no in-process common ground, and the only honest way to prove they agree
// is to put a live instance of each on the same runtime. Go drives it over
// stdio — one JSON object per line — so the test reads the Node side's
// observations as ordinary values.

type agentConfig struct {
	URL              string     `json:"url"`
	Key              string     `json:"key"`
	DataDir          string     `json:"dataDir"`
	ProtoFile        string     `json:"protoFile"`
	RPCMethod        string     `json:"rpcMethod"`
	SubOpSubject     string     `json:"subOpSubject"`
	CallSubOpSubject string     `json:"callSubOpSubject"`
	Deps             []agentDep `json:"deps"`
	SubscribeEvents  []string   `json:"subscribeEvents"`
	PublishEvents    []string   `json:"publishEvents"`
	// JobName declares a scheduled job with JobOpts (a raw JobOpts JSON value,
	// camelCase, matching node/src/job/types.ts). Empty registers none.
	JobName string `json:"jobName"`
	JobOpts any    `json:"jobOpts"`
	// WorkflowName declares a workflow whose only step is a `call` reaching
	// WorkflowCallService/WorkflowCallMethod. Empty declares none.
	WorkflowName        string `json:"workflowName"`
	WorkflowCallService string `json:"workflowCallService"`
	WorkflowCallMethod  string `json:"workflowCallMethod"`
}

type agentDep struct {
	Service string   `json:"service"`
	Methods []string `json:"methods"`
}

type agentReady struct {
	ServiceName       string `json:"serviceName"`
	ServiceID         string `json:"serviceId"`
	InstanceID        string `json:"instanceId"`
	RPCContractHash   string `json:"rpcContractHash"`
	EventContractHash string `json:"eventContractHash"`
}

type agentEvent struct {
	Name    string         `json:"name"`
	Payload map[string]any `json:"payload"`
}

type agentRPC struct {
	Method string         `json:"method"`
	Req    map[string]any `json:"req"`
}

// agentJob is one execution the agent's job handler was invoked for.
type agentJob struct {
	Name           string
	Attempt        int
	IdempotencyKey string
}

type agentMessage struct {
	Type    string          `json:"type"`
	ID      int64           `json:"id"`
	OK      bool            `json:"ok"`
	Error   string          `json:"error"`
	Value   json.RawMessage `json:"value"`
	Name    string          `json:"name"`
	Method  string          `json:"method"`
	Payload map[string]any  `json:"payload"`
	Req     map[string]any  `json:"req"`

	ServiceName       string `json:"serviceName"`
	ServiceID         string `json:"serviceId"`
	InstanceID        string `json:"instanceId"`
	RPCContractHash   string `json:"rpcContractHash"`
	EventContractHash string `json:"eventContractHash"`

	Attempt        int    `json:"attempt"`
	IdempotencyKey string `json:"idempotencyKey"`
}

type nodeAgent struct {
	t      *testing.T
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stderr *bytes.Buffer

	Ready agentReady

	events chan agentEvent
	rpcs   chan agentRPC
	jobs   chan agentJob

	mu      sync.Mutex
	results map[int64]chan agentMessage
	nextID  atomic.Int64
}

// startNodeAgent launches the agent and blocks until it reports a live session.
// A failure to come up is fatal here rather than at the first assertion: the
// agent's stderr is the only place a Node-side stack trace exists.
func startNodeAgent(ctx context.Context, t *testing.T, cfg agentConfig) *nodeAgent {
	t.Helper()

	bun, err := exec.LookPath("bun")
	if err != nil {
		t.Fatalf("bun is not on PATH: the cross-language tests run the Node SDK through it: %v", err)
	}
	root, err := repoDir()
	if err != nil {
		t.Fatalf("resolve package directory: %v", err)
	}
	script := filepath.Join(root, "nodeagent", "agent.ts")
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("agent script %s is missing: %v", script, err)
	}
	nodeSrc := filepath.Join(suite.sdkRepo, "node", "src")
	if _, err := os.Stat(nodeSrc); err != nil {
		t.Fatalf("the Node SDK sources are not at %s: %v", nodeSrc, err)
	}

	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("encode agent configuration: %v", err)
	}

	cmd := exec.CommandContext(ctx, bun, "run", script)
	cmd.Dir = filepath.Join(suite.sdkRepo, "node")
	cmd.Env = append(os.Environ(),
		"SB_AGENT_CONFIG="+string(encoded),
		"SB_NODE_SDK_SRC="+nodeSrc,
	)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("open agent stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("open agent stdout: %v", err)
	}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr

	a := &nodeAgent{
		t:       t,
		cmd:     cmd,
		stdin:   stdin,
		stderr:  stderr,
		events:  make(chan agentEvent, 64),
		rpcs:    make(chan agentRPC, 64),
		jobs:    make(chan agentJob, 64),
		results: map[int64]chan agentMessage{},
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start agent: %v", err)
	}

	ready := make(chan agentReady, 1)
	fatal := make(chan string, 1)
	go a.read(stdout, ready, fatal)

	t.Cleanup(func() { a.stop() })

	select {
	case a.Ready = <-ready:
	case msg := <-fatal:
		t.Fatalf("agent failed before it was ready: %s\nstderr:\n%s", msg, stderr.String())
	case <-time.After(connectTimeout + 20*time.Second):
		t.Fatalf("agent did not report a live session within %s\nstderr:\n%s",
			connectTimeout+20*time.Second, stderr.String())
	}
	return a
}

// read routes the agent's output. Every line is one message; anything that is
// not JSON is the Node runtime writing to stdout, which is a failure the test
// has to see rather than skip over.
func (a *nodeAgent) read(stdout io.Reader, ready chan<- agentReady, fatal chan<- string) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var msg agentMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			a.t.Logf("agent wrote a non-message line: %s", line)
			continue
		}
		switch msg.Type {
		case "ready":
			ready <- agentReady{
				ServiceName:       msg.ServiceName,
				ServiceID:         msg.ServiceID,
				InstanceID:        msg.InstanceID,
				RPCContractHash:   msg.RPCContractHash,
				EventContractHash: msg.EventContractHash,
			}
		case "event":
			a.events <- agentEvent{Name: msg.Name, Payload: msg.Payload}
		case "rpc":
			a.rpcs <- agentRPC{Method: msg.Method, Req: msg.Req}
		case "job":
			a.jobs <- agentJob{Name: msg.Name, Attempt: msg.Attempt, IdempotencyKey: msg.IdempotencyKey}
		case "result":
			a.mu.Lock()
			ch, ok := a.results[msg.ID]
			delete(a.results, msg.ID)
			a.mu.Unlock()
			if ok {
				ch <- msg
			}
		case "fatal":
			fatal <- msg.Error
		}
	}
	// Reaching here means the agent closed its stdout. If it never announced a
	// session, waiting out the connect timeout would only delay the same
	// failure and bury the stderr that explains it.
	select {
	case fatal <- "the agent exited before it reported a live session":
	default:
	}
}

// send writes one command and waits for the answer carrying the same id.
func (a *nodeAgent) send(ctx context.Context, cmd map[string]any) (agentMessage, error) {
	id := a.nextID.Add(1)
	cmd["id"] = id

	ch := make(chan agentMessage, 1)
	a.mu.Lock()
	a.results[id] = ch
	a.mu.Unlock()

	encoded, err := json.Marshal(cmd)
	if err != nil {
		return agentMessage{}, fmt.Errorf("encode agent command: %w", err)
	}
	if _, err := a.stdin.Write(append(encoded, '\n')); err != nil {
		return agentMessage{}, fmt.Errorf("write agent command: %w", err)
	}

	select {
	case msg := <-ch:
		if !msg.OK {
			return msg, fmt.Errorf("agent command %v failed: %s", cmd["cmd"], msg.Error)
		}
		return msg, nil
	case <-ctx.Done():
		return agentMessage{}, fmt.Errorf("agent command %v: %w (stderr: %s)", cmd["cmd"], ctx.Err(), a.stderr.String())
	}
}

// awaitMethod blocks until the agent's own mesh view carries the method. The Go
// side's readiness says nothing about the Node side's: each instance gets its
// own registry snapshot.
func (a *nodeAgent) awaitMethod(ctx context.Context, t *testing.T, service, method string) {
	t.Helper()
	if _, err := a.send(ctx, map[string]any{
		"cmd": "awaitMethod", "service": service, "method": method,
	}); err != nil {
		t.Fatalf("the Node agent never discovered %s/%s: %v\nagent stderr:\n%s",
			service, method, err, a.stderr.String())
	}
}

// call makes the Node instance call a Go handler and returns the decoded reply.
func (a *nodeAgent) call(ctx context.Context, service, method string, payload map[string]any) (map[string]any, error) {
	msg, err := a.send(ctx, map[string]any{
		"cmd": "call", "service": service, "method": method, "payload": payload,
	})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(msg.Value, &out); err != nil {
		return nil, fmt.Errorf("decode agent call reply %s: %w", msg.Value, err)
	}
	return out, nil
}

// publish makes the Node instance publish an event.
func (a *nodeAgent) publish(ctx context.Context, name string, payload map[string]any) error {
	_, err := a.send(ctx, map[string]any{"cmd": "publish", "name": name, "payload": payload})
	return err
}

// waitEvent blocks for one delivery the agent observed.
func (a *nodeAgent) waitEvent(t *testing.T, timeout time.Duration) agentEvent {
	t.Helper()
	select {
	case e := <-a.events:
		return e
	case <-time.After(timeout):
		t.Fatalf("the Node agent received no event within %s\nstderr:\n%s", timeout, a.stderr.String())
		return agentEvent{}
	}
}

// waitRPC blocks for one call the agent's handler served.
func (a *nodeAgent) waitRPC(t *testing.T, timeout time.Duration) agentRPC {
	t.Helper()
	select {
	case r := <-a.rpcs:
		return r
	case <-time.After(timeout):
		t.Fatalf("the Node agent's handler was not called within %s\nstderr:\n%s", timeout, a.stderr.String())
		return agentRPC{}
	}
}

// waitJob blocks for one execution the agent's job handler served.
func (a *nodeAgent) waitJob(t *testing.T, timeout time.Duration) agentJob {
	t.Helper()
	select {
	case j := <-a.jobs:
		return j
	case <-time.After(timeout):
		t.Fatalf("the Node agent's job handler was not called within %s\nstderr:\n%s", timeout, a.stderr.String())
		return agentJob{}
	}
}

// startWorkflow makes the Node instance start a run of its declared workflow.
func (a *nodeAgent) startWorkflow(ctx context.Context, name string, payload map[string]any) (string, error) {
	msg, err := a.send(ctx, map[string]any{"cmd": "startWorkflow", "name": name, "payload": payload})
	if err != nil {
		return "", err
	}
	var out struct {
		RunID string `json:"runId"`
	}
	if err := json.Unmarshal(msg.Value, &out); err != nil {
		return "", fmt.Errorf("decode agent startWorkflow reply %s: %w", msg.Value, err)
	}
	return out.RunID, nil
}

// awaitWorkflow blocks until the Node instance's run reaches a terminal state
// and returns the final state map.
func (a *nodeAgent) awaitWorkflow(ctx context.Context, runID string) (map[string]any, error) {
	msg, err := a.send(ctx, map[string]any{"cmd": "awaitWorkflow", "runId": runID})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(msg.Value, &out); err != nil {
		return nil, fmt.Errorf("decode agent awaitWorkflow reply %s: %w", msg.Value, err)
	}
	return out, nil
}

// nodeWorkflowFingerprint runs nodeagent/workflow-fingerprint.ts once, out of
// process, and returns the Node SDK's own canonical JSON + fingerprint for
// graph. It needs no runtime connection: canonicalize/fingerprint are pure
// functions of the graph, so this is the fastest, most direct way to prove the
// two SDKs render byte-identical bytes for the same workflow — a runtime round
// trip would only prove the graph parses, not that the bytes matched.
func nodeWorkflowFingerprint(ctx context.Context, t *testing.T, graph any) (canonicalJSON, fingerprint string) {
	t.Helper()

	bun, err := exec.LookPath("bun")
	if err != nil {
		t.Fatalf("bun is not on PATH: %v", err)
	}
	root, err := repoDir()
	if err != nil {
		t.Fatalf("resolve package directory: %v", err)
	}
	script := filepath.Join(root, "nodeagent", "workflow-fingerprint.ts")
	nodeSrc := filepath.Join(suite.sdkRepo, "node", "src")

	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("encode graph: %v", err)
	}

	cmd := exec.CommandContext(ctx, bun, "run", script, nodeSrc, string(graphJSON))
	cmd.Dir = filepath.Join(suite.sdkRepo, "node")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("workflow-fingerprint.ts failed: %v\nstderr:\n%s", err, stderr.String())
	}

	var out struct {
		Canonical   string `json:"canonical"`
		Fingerprint string `json:"fingerprint"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &out); err != nil {
		t.Fatalf("decode workflow-fingerprint.ts output %s: %v", stdout.String(), err)
	}
	return out.Canonical, out.Fingerprint
}

// stop asks the agent to close its client, then makes sure the process is gone:
// a surviving agent keeps the identity's Events.Subscribe stream and the next
// test on it would be refused.
func (a *nodeAgent) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := a.send(ctx, map[string]any{"cmd": "stop"}); err != nil {
		a.t.Logf("agent stop: %v", err)
	}
	_ = a.stdin.Close()

	done := make(chan error, 1)
	go func() { done <- a.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = a.cmd.Process.Kill()
		<-done
	}
	if a.t.Failed() && a.stderr.Len() > 0 {
		a.t.Logf("agent stderr:\n%s", a.stderr.String())
	}
}
