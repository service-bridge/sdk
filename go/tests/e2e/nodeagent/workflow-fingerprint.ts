// workflow-fingerprint.ts — computes the Node SDK's canonical JSON + fingerprint
// for a workflow graph, using the SDK's own exported canonical.ts functions.
//
// This is deliberately NOT part of agent.ts: fingerprinting a graph needs no
// connection to a runtime, and running it as a one-shot script keeps the
// comparison test fast and immune to connect flakiness. It exists to prove
// go/internal/workflow/canonical.go and node/src/workflow/canonical.ts render
// the identical byte-for-byte JSON for the same graph — a divergence here
// (key order, casing, omitted-vs-null fields) silently changes contract_hash
// on one side only and breaks cross-language workflow registration.
//
// Usage: bun run workflow-fingerprint.ts <SB_NODE_SDK_SRC> <canonicalGraphJSON>
// Prints one line of JSON: {"canonical": "...", "fingerprint": "..."}.

const sdkSrc = process.argv[2];
const graphArg = process.argv[3];
if (!sdkSrc || !graphArg) {
	throw new Error("usage: workflow-fingerprint.ts <sdkSrc> <graphJSON>");
}

const { canonicalize, fingerprint } = await import(`${sdkSrc}/workflow/canonical.ts`);

const graph = JSON.parse(graphArg);
process.stdout.write(
	`${JSON.stringify({ canonical: canonicalize(graph), fingerprint: fingerprint(graph) })}\n`,
);
