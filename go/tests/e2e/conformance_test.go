//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
)

// The conformance scenarios (sdk/conformance/scenarios/*.yaml) state SDK
// behaviour once, for both SDKs. Every scenario runs for each pairing of
// caller and callee SDK against the live runtime; an expectation that holds
// for one pairing and not another names the SDK that diverges.

const domainConformance = "conformance"

// runtimeNext is set by conformance_next_test.go under the runtime_next tag.
// Scenarios marked `requires: runtime-next` need a runtime from that line and
// are left out of a build without the tag, as the Go tests of the same kind.
var runtimeNext bool

type confFile struct {
	Scenarios []confScenario `yaml:"scenarios"`
}

type confScenario struct {
	Name     string     `yaml:"name"`
	Requires string     `yaml:"requires"`
	Caller   confCaller `yaml:"caller"`
	Callee   confCallee `yaml:"callee"`
	Steps    []confStep `yaml:"steps"`

	file string
	// ns prefixes every method, event and pattern of this scenario in one
	// pairing, so scenarios and pairings never see each other's traffic.
	ns string
}

type confCaller struct {
	Deps   []string `yaml:"deps"`
	Events []string `yaml:"events"`
}

type confCallee struct {
	RPC           []confHandler      `yaml:"rpc"`
	Subscriptions []confSubscription `yaml:"subscriptions"`
}

type confHandler struct {
	Method    string        `yaml:"method" json:"method"`
	Behaviour confBehaviour `yaml:"behaviour" json:"behaviour"`
}

// confBehaviour is what a callee handler does. In YAML it is either a bare
// word (echo, info) or a one-key map.
type confBehaviour struct {
	Echo    bool      `json:"echo,omitempty"`
	Info    bool      `json:"info,omitempty"`
	SleepMs int       `yaml:"sleepMs" json:"sleepMs,omitempty"`
	Fail    *confFail `yaml:"fail" json:"fail,omitempty"`
	Throw   *string   `yaml:"throw" json:"throw,omitempty"`
}

func (b *confBehaviour) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		switch n.Value {
		case "echo":
			b.Echo = true
		case "info":
			b.Info = true
		default:
			return fmt.Errorf("line %d: unknown behaviour %q", n.Line, n.Value)
		}
		return nil
	}
	type plain confBehaviour
	return n.Decode((*plain)(b))
}

type confFail struct {
	Code    string `yaml:"code" json:"code"`
	Message string `yaml:"message" json:"message"`
}

type confSubscription struct {
	Pattern string         `yaml:"pattern" json:"pattern"`
	Filter  map[string]any `yaml:"filter" json:"filter,omitempty"`
}

type confStep struct {
	Call       *confCall         `yaml:"call"`
	Publish    *confPublish      `yaml:"publish"`
	Deliveries []confDeliveryExp `yaml:"deliveries"`
	Expect     confExpect        `yaml:"expect"`
}

type confCall struct {
	Method         string         `yaml:"method"`
	Transport      string         `yaml:"transport"`
	TimeoutMs      int            `yaml:"timeoutMs"`
	IdempotencyKey string         `yaml:"idempotencyKey"`
	Payload        map[string]any `yaml:"payload"`
}

type confPublish struct {
	Event        string         `yaml:"event"`
	PartitionKey string         `yaml:"partitionKey"`
	Payload      map[string]any `yaml:"payload"`
}

type confExpect struct {
	Reply  *confReplyExp `yaml:"reply"`
	Error  *confErrorExp `yaml:"error"`
	Served *int          `yaml:"served"`
}

type confReplyExp struct {
	Text      *string `yaml:"text"`
	N         *int64  `yaml:"n"`
	NMin      *int64  `yaml:"nMin"`
	NMax      *int64  `yaml:"nMax"`
	HandledBy *string `yaml:"handledBy"`
}

type confErrorExp struct {
	Code           string  `yaml:"code"`
	HandlerCode    *string `yaml:"handlerCode"`
	HandlerMessage *string `yaml:"handlerMessage"`
	Retryable      *bool   `yaml:"retryable"`
}

type confDeliveryExp struct {
	Pattern  string   `yaml:"pattern"`
	Event    string   `yaml:"event"`
	OrderID  string   `yaml:"orderId"`
	Amount   *float64 `yaml:"amount"`
	Currency *string  `yaml:"currency"`
}

// confOutcome is what an agent reports for a call or a publish: exactly one
// of Reply, Error or EventID is set.
type confOutcome struct {
	Reply *struct {
		Text      string `json:"text"`
		N         int64  `json:"n"`
		HandledBy string `json:"handledBy"`
	} `json:"reply"`
	Error *struct {
		Code           string `json:"code"`
		HandlerCode    string `json:"handlerCode"`
		HandlerMessage string `json:"handlerMessage"`
		Retryable      bool   `json:"retryable"`
		Message        string `json:"message"`
	} `json:"error"`
	EventID string `json:"eventId"`
}

type confDelivery struct {
	Pattern  string
	Event    string
	OrderID  string
	Amount   float64
	Currency string
}

// confAgentConfig is one role's declarations, the same JSON for both SDKs.
type confAgentConfig struct {
	URL           string             `json:"url"`
	Key           string             `json:"key"`
	ProtoFile     string             `json:"protoFile"`
	RPC           []confHandler      `json:"rpc"`
	Subscriptions []confSubscription `json:"subscriptions"`
	Deps          []agentDep         `json:"deps"`
	Events        []string           `json:"events"`
}

type confCallOpts struct {
	Transport      string
	TimeoutMs      int
	IdempotencyKey string
}

// confAgent is one SDK playing one role.
type confAgent interface {
	sdk() string
	serviceID() string
	serviceName() string
	awaitMethod(ctx context.Context, t *testing.T, service, method string)
	call(ctx context.Context, service, method string, payload map[string]any, opts confCallOpts) (confOutcome, error)
	publish(ctx context.Context, name, partitionKey string, payload map[string]any) (confOutcome, error)
	deliveries() []confDelivery
	servedCount(method string) int
}

func loadScenarios(t *testing.T) []confScenario {
	t.Helper()
	dir := filepath.Join(suite.sdkRepo, "conformance", "scenarios")
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no conformance scenarios in %s: %v", dir, err)
	}
	sort.Strings(files)
	var out []confScenario
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		var f confFile
		dec := yaml.NewDecoder(strings.NewReader(string(raw)))
		dec.KnownFields(true)
		if err := dec.Decode(&f); err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, s := range f.Scenarios {
			if s.Requires != "" && s.Requires != "runtime-next" {
				t.Fatalf("%s: scenario %q requires unknown %q", path, s.Name, s.Requires)
			}
			if s.Requires == "runtime-next" && !runtimeNext {
				t.Logf("%s: %q needs -tags runtime_next, left out", filepath.Base(path), s.Name)
				continue
			}
			s.file = filepath.Base(path)
			out = append(out, s)
		}
	}
	return out
}

// TestConformance runs every scenario for every pairing of SDKs.
func TestConformance(t *testing.T) {
	scenarios := loadScenarios(t)
	for _, callerSDK := range []string{"go", "node"} {
		for _, calleeSDK := range []string{"go", "node"} {
			t.Run(callerSDK+"_calls_"+calleeSDK, func(t *testing.T) {
				runPairing(t, scenarios, callerSDK, calleeSDK)
			})
		}
	}
}

func runPairing(t *testing.T, scenarios []confScenario, callerSDK, calleeSDK string) {
	ctx := testContext(t, 5*time.Minute)

	pairing := uniqueName("conf")
	for i := range scenarios {
		scenarios[i].ns = fmt.Sprintf("%s.s%d", pairing, i)
	}

	calleeName := serviceName(domainConformance, 2)
	callee := confAgentConfig{
		URL: suite.runtimeURL, Key: bootstrapKey(t, domainConformance, 2), ProtoFile: suite.protoFile,
	}
	caller := confAgentConfig{
		URL: suite.runtimeURL, Key: bootstrapKey(t, domainConformance, 1), ProtoFile: suite.protoFile,
	}
	var calleeMethods, callerMethods []string
	for _, s := range scenarios {
		for _, h := range s.Callee.RPC {
			m := s.ns + "." + h.Method
			callee.RPC = append(callee.RPC, confHandler{Method: m, Behaviour: h.Behaviour})
			calleeMethods = append(calleeMethods, m)
			callerMethods = append(callerMethods, m)
		}
		for _, d := range s.Caller.Deps {
			callerMethods = append(callerMethods, s.ns+"."+d)
		}
		for _, sub := range s.Callee.Subscriptions {
			callee.Subscriptions = append(callee.Subscriptions,
				confSubscription{Pattern: s.ns + "." + sub.Pattern, Filter: sub.Filter})
		}
		for _, e := range s.Caller.Events {
			caller.Events = append(caller.Events, s.ns+"."+e)
		}
	}
	slices.Sort(callerMethods)
	caller.Deps = []agentDep{{Service: calleeName, Methods: slices.Compact(callerMethods)}}

	calleeAgent := startConfAgent(ctx, t, calleeSDK, callee)
	callerAgent := startConfAgent(ctx, t, callerSDK, caller)
	for _, m := range calleeMethods {
		callerAgent.awaitMethod(ctx, t, calleeName, m)
	}

	for _, s := range scenarios {
		t.Run(s.file+"/"+s.Name, func(t *testing.T) {
			vars := map[string]string{
				"$caller.serviceId": callerAgent.serviceID(),
				"$callee.sdk":       calleeAgent.sdk(),
			}
			for i, step := range s.Steps {
				runStep(ctx, t, s, i, step, callerAgent, calleeAgent, calleeName, vars)
			}
		})
	}
}

func runStep(ctx context.Context, t *testing.T, s confScenario, i int, step confStep,
	caller, callee confAgent, calleeName string, vars map[string]string,
) {
	t.Helper()
	where := fmt.Sprintf("step %d (caller %s, callee %s)", i+1, caller.sdk(), callee.sdk())
	switch {
	case step.Call != nil:
		c := step.Call
		method := s.ns + "." + c.Method
		callCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		got, err := caller.call(callCtx, calleeName, method, c.Payload,
			confCallOpts{Transport: c.Transport, TimeoutMs: c.TimeoutMs, IdempotencyKey: c.IdempotencyKey})
		if err != nil {
			t.Fatalf("%s: call %s: %v", where, c.Method, err)
		}
		checkOutcome(t, where, got, step.Expect, vars)
		if step.Expect.Served != nil {
			if n := callee.servedCount(method); n != *step.Expect.Served {
				t.Errorf("%s: the handler ran %d times, want %d", where, n, *step.Expect.Served)
			}
		}
	case step.Publish != nil:
		p := step.Publish
		got, err := caller.publish(ctx, s.ns+"."+p.Event, p.PartitionKey, p.Payload)
		if err != nil {
			t.Fatalf("%s: publish %s: %v", where, p.Event, err)
		}
		if got.Error != nil || got.EventID == "" {
			t.Fatalf("%s: publish %s failed: %+v", where, p.Event, got.Error)
		}
	case step.Deliveries != nil:
		checkDeliveries(ctx, t, where, s.ns, step.Deliveries, callee)
	default:
		t.Fatalf("%s: a step needs call, publish or deliveries", where)
	}
}

func resolve(v string, vars map[string]string) string {
	if r, ok := vars[v]; ok {
		return r
	}
	return v
}

func checkOutcome(t *testing.T, where string, got confOutcome, want confExpect, vars map[string]string) {
	t.Helper()
	if want.Reply != nil {
		if got.Reply == nil {
			t.Fatalf("%s: want a reply, got error %+v", where, got.Error)
		}
		r, w := got.Reply, want.Reply
		if w.Text != nil && r.Text != resolve(*w.Text, vars) {
			t.Errorf("%s: text = %q, want %q", where, r.Text, resolve(*w.Text, vars))
		}
		if w.HandledBy != nil && r.HandledBy != resolve(*w.HandledBy, vars) {
			t.Errorf("%s: handledBy = %q, want %q", where, r.HandledBy, resolve(*w.HandledBy, vars))
		}
		if w.N != nil && r.N != *w.N {
			t.Errorf("%s: n = %d, want %d", where, r.N, *w.N)
		}
		if w.NMin != nil && r.N < *w.NMin {
			t.Errorf("%s: n = %d, want at least %d", where, r.N, *w.NMin)
		}
		if w.NMax != nil && r.N > *w.NMax {
			t.Errorf("%s: n = %d, want at most %d", where, r.N, *w.NMax)
		}
	}
	if want.Error != nil {
		if got.Error == nil {
			t.Fatalf("%s: want error %s, got reply %+v", where, want.Error.Code, got.Reply)
		}
		e, w := got.Error, want.Error
		if e.Code != w.Code {
			t.Errorf("%s: code = %s, want %s (message %q)", where, e.Code, w.Code, e.Message)
		}
		if w.HandlerCode != nil && e.HandlerCode != *w.HandlerCode {
			t.Errorf("%s: handler code = %q, want %q", where, e.HandlerCode, *w.HandlerCode)
		}
		if w.HandlerMessage != nil && e.HandlerMessage != *w.HandlerMessage {
			t.Errorf("%s: handler message = %q, want %q", where, e.HandlerMessage, *w.HandlerMessage)
		}
		if w.Retryable != nil && e.Retryable != *w.Retryable {
			t.Errorf("%s: retryable = %v, want %v", where, e.Retryable, *w.Retryable)
		}
	}
}

// checkDeliveries waits for the expected set, then gives stragglers a short
// window: an event that must not arrive (a filtered one) shows up only as an
// extra delivery, and there is no event to wait for instead.
func checkDeliveries(ctx context.Context, t *testing.T, where, ns string, want []confDeliveryExp, callee confAgent) {
	t.Helper()
	ofScenario := func() []confDelivery {
		seen := map[confDelivery]bool{}
		var out []confDelivery
		for _, d := range callee.deliveries() {
			if !strings.HasPrefix(d.Event, ns+".") || seen[d] {
				continue
			}
			seen[d] = true
			out = append(out, d)
		}
		return out
	}
	matches := func(d confDelivery, w confDeliveryExp) bool {
		return d.Pattern == ns+"."+w.Pattern && d.Event == ns+"."+w.Event && d.OrderID == w.OrderID &&
			(w.Amount == nil || d.Amount == *w.Amount) && (w.Currency == nil || d.Currency == *w.Currency)
	}
	allArrived := func() bool {
		got := ofScenario()
		for _, w := range want {
			if !slices.ContainsFunc(got, func(d confDelivery) bool { return matches(d, w) }) {
				return false
			}
		}
		return true
	}
	waitFor(ctx, t, deliveryTimeout, where+": expected deliveries", func(context.Context) (bool, error) {
		return allArrived(), nil
	})
	time.Sleep(time.Second)
	for _, d := range ofScenario() {
		if !slices.ContainsFunc(want, func(w confDeliveryExp) bool { return matches(d, w) }) {
			t.Errorf("%s: unexpected delivery %+v", where, d)
		}
	}
}

func startConfAgent(ctx context.Context, t *testing.T, sdk string, cfg confAgentConfig) confAgent {
	t.Helper()
	if sdk == "node" {
		return &nodeConfAgent{spawnNodeAgent(ctx, t, "conformance.ts", cfg)}
	}
	return startGoConfAgent(ctx, t, cfg)
}

// ── Node ────────────────────────────────────────────────────────────────────

type nodeConfAgent struct{ a *nodeAgent }

func (n *nodeConfAgent) sdk() string         { return "node" }
func (n *nodeConfAgent) serviceID() string   { return n.a.Ready.ServiceID }
func (n *nodeConfAgent) serviceName() string { return n.a.Ready.ServiceName }

func (n *nodeConfAgent) awaitMethod(ctx context.Context, t *testing.T, service, method string) {
	n.a.awaitMethod(ctx, t, service, method)
}

func (n *nodeConfAgent) call(ctx context.Context, service, method string, payload map[string]any, opts confCallOpts) (confOutcome, error) {
	cmd := map[string]any{"cmd": "call", "service": service, "method": method, "payload": payload}
	if opts.Transport != "" {
		cmd["transport"] = opts.Transport
	}
	if opts.TimeoutMs > 0 {
		cmd["timeoutMs"] = opts.TimeoutMs
	}
	if opts.IdempotencyKey != "" {
		cmd["idempotencyKey"] = opts.IdempotencyKey
	}
	return n.outcome(ctx, cmd)
}

func (n *nodeConfAgent) publish(ctx context.Context, name, partitionKey string, payload map[string]any) (confOutcome, error) {
	return n.outcome(ctx, map[string]any{"cmd": "publish", "name": name, "partitionKey": partitionKey, "payload": payload})
}

func (n *nodeConfAgent) outcome(ctx context.Context, cmd map[string]any) (confOutcome, error) {
	msg, err := n.a.send(ctx, cmd)
	if err != nil {
		return confOutcome{}, err
	}
	var out confOutcome
	if err := json.Unmarshal(msg.Value, &out); err != nil {
		return confOutcome{}, fmt.Errorf("decode %s: %w", msg.Value, err)
	}
	return out, nil
}

func (n *nodeConfAgent) deliveries() []confDelivery {
	n.a.mu.Lock()
	defer n.a.mu.Unlock()
	return slices.Clone(n.a.deliveries)
}

func (n *nodeConfAgent) servedCount(method string) int {
	n.a.mu.Lock()
	defer n.a.mu.Unlock()
	return n.a.served[method]
}

// ── Go ──────────────────────────────────────────────────────────────────────

type goConfAgent struct {
	c *servicebridge.Client

	mu     sync.Mutex
	got    []confDelivery
	served map[string]int
	events map[string]*servicebridge.Event[*e2epb.OrderEvent]
}

func startGoConfAgent(ctx context.Context, t *testing.T, cfg confAgentConfig) *goConfAgent {
	t.Helper()
	c, err := servicebridge.New(cfg.URL, cfg.Key,
		servicebridge.WithLogger(logger(t)),
		servicebridge.WithReconnectAttempts(3),
		servicebridge.WithAdvertise("127.0.0.1", 0))
	if err != nil {
		t.Fatalf("new Go client: %v", err)
	}
	g := &goConfAgent{c: c, served: map[string]int{}, events: map[string]*servicebridge.Event[*e2epb.OrderEvent]{}}
	for _, h := range cfg.RPC {
		if err := servicebridge.Handle(c, h.Method, g.handler(h.Method, h.Behaviour)); err != nil {
			t.Fatalf("declare %s: %v", h.Method, err)
		}
	}
	for _, s := range cfg.Subscriptions {
		var opts []servicebridge.SubscribeOption
		if s.Filter != nil {
			opts = append(opts, servicebridge.WithFilter(s.Filter))
		}
		if err := servicebridge.SubscribeEvent(c, s.Pattern, g.subscriber(s.Pattern), opts...); err != nil {
			t.Fatalf("subscribe %s: %v", s.Pattern, err)
		}
	}
	for _, d := range cfg.Deps {
		if err := c.Service(d.Service, servicebridge.ServiceDeps{RPC: d.Methods}); err != nil {
			t.Fatalf("declare dependency %s: %v", d.Service, err)
		}
	}
	for _, name := range cfg.Events {
		e, err := servicebridge.DefineEvent[*e2epb.OrderEvent](c, name)
		if err != nil {
			t.Fatalf("define %s: %v", name, err)
		}
		g.events[name] = e
	}
	start(ctx, t, c)
	return g
}

func (g *goConfAgent) handler(method string, b confBehaviour) func(context.Context, *e2epb.Echo) (*e2epb.EchoReply, error) {
	return func(ctx context.Context, req *e2epb.Echo) (*e2epb.EchoReply, error) {
		g.mu.Lock()
		g.served[method]++
		g.mu.Unlock()
		switch {
		case b.Fail != nil:
			return nil, &servicebridge.HandlerError{Code: b.Fail.Code, Message: b.Fail.Message}
		case b.Throw != nil:
			return nil, errors.New(*b.Throw)
		case b.Info:
			info, _ := servicebridge.CallInfoFromContext(ctx)
			var remaining int64
			if !info.Deadline.IsZero() {
				remaining = time.Until(info.Deadline).Milliseconds()
			}
			return &e2epb.EchoReply{Text: info.CallerServiceID, N: remaining, HandledBy: info.IdempotencyKey}, nil
		}
		if b.SleepMs > 0 {
			select {
			case <-time.After(time.Duration(b.SleepMs) * time.Millisecond):
			case <-ctx.Done():
			}
		}
		return &e2epb.EchoReply{Text: req.GetText(), N: req.GetN(), HandledBy: "go"}, nil
	}
}

func (g *goConfAgent) subscriber(pattern string) func(context.Context, *e2epb.OrderEvent) error {
	return func(ctx context.Context, e *e2epb.OrderEvent) error {
		info, _ := servicebridge.DeliveryFromContext(ctx)
		g.mu.Lock()
		g.got = append(g.got, confDelivery{
			Pattern: pattern, Event: info.EventName,
			OrderID: e.GetOrderId(), Amount: e.GetAmount(), Currency: e.GetCurrency(),
		})
		g.mu.Unlock()
		return nil
	}
}

func (g *goConfAgent) sdk() string         { return "go" }
func (g *goConfAgent) serviceID() string   { return g.c.Identity().ServiceID }
func (g *goConfAgent) serviceName() string { return g.c.Identity().ServiceName }

func (g *goConfAgent) awaitMethod(ctx context.Context, t *testing.T, service, method string) {
	waitForMethod(ctx, t, g.c, service, method)
}

func (g *goConfAgent) call(ctx context.Context, service, method string, payload map[string]any, opts confCallOpts) (confOutcome, error) {
	req := &e2epb.Echo{}
	if v, ok := payload["text"].(string); ok {
		req.Text = v
	}
	if v, ok := payload["n"].(int); ok {
		req.N = int64(v)
	}
	var callOpts []servicebridge.CallOption
	switch opts.Transport {
	case "":
	case "direct":
		callOpts = append(callOpts, servicebridge.WithTransport(servicebridge.TransportDirect))
	case "proxy":
		callOpts = append(callOpts, servicebridge.WithTransport(servicebridge.TransportProxy))
	case "auto":
		callOpts = append(callOpts, servicebridge.WithTransport(servicebridge.TransportAuto))
	default:
		return confOutcome{}, fmt.Errorf("unknown transport %q", opts.Transport)
	}
	if opts.TimeoutMs > 0 {
		callOpts = append(callOpts, servicebridge.WithTimeout(time.Duration(opts.TimeoutMs)*time.Millisecond))
	}
	if opts.IdempotencyKey != "" {
		callOpts = append(callOpts, servicebridge.WithIdempotencyKey(opts.IdempotencyKey))
	}
	resp, err := servicebridge.Call[*e2epb.Echo, *e2epb.EchoReply](ctx, g.c, service, method, req, callOpts...)
	var out confOutcome
	if err == nil {
		out.Reply = &struct {
			Text      string `json:"text"`
			N         int64  `json:"n"`
			HandledBy string `json:"handledBy"`
		}{resp.GetText(), resp.GetN(), resp.GetHandledBy()}
		return out, nil
	}
	return goErrorOutcome(err), nil
}

func goErrorOutcome(err error) confOutcome {
	var out confOutcome
	out.Error = &struct {
		Code           string `json:"code"`
		HandlerCode    string `json:"handlerCode"`
		HandlerMessage string `json:"handlerMessage"`
		Retryable      bool   `json:"retryable"`
		Message        string `json:"message"`
	}{Code: "NOT_AN_SDK_ERROR", Message: err.Error()}
	var sbErr *servicebridge.Error
	if errors.As(err, &sbErr) {
		out.Error.Code = string(sbErr.Code)
		out.Error.Retryable = sbErr.Retryable()
	}
	var hErr *servicebridge.HandlerError
	if errors.As(err, &hErr) {
		out.Error.HandlerCode = hErr.Code
		out.Error.HandlerMessage = hErr.Message
	}
	return out
}

func (g *goConfAgent) publish(ctx context.Context, name, partitionKey string, payload map[string]any) (confOutcome, error) {
	e, ok := g.events[name]
	if !ok {
		return confOutcome{}, fmt.Errorf("event %s is not declared on this agent", name)
	}
	ev := &e2epb.OrderEvent{}
	if v, ok := payload["orderId"].(string); ok {
		ev.OrderId = v
	}
	if v, ok := payload["currency"].(string); ok {
		ev.Currency = v
	}
	ev.Amount = toFloat(payload["amount"])
	var opts []servicebridge.PublishOption
	if partitionKey != "" {
		opts = append(opts, servicebridge.WithPartitionKey(partitionKey))
	}
	id, err := e.Publish(ctx, ev, opts...)
	if err != nil {
		return goErrorOutcome(err), nil
	}
	return confOutcome{EventID: id}, nil
}

func (g *goConfAgent) deliveries() []confDelivery {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.got)
}

func (g *goConfAgent) servedCount(method string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.served[method]
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	}
	return 0
}
