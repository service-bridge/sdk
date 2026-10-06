package connection

// Handshake identity the SDK sends in OpenRequest and RegisterRequest. The
// runtime refuses any protocol revision it does not speak with
// FAILED_PRECONDITION, which the lifecycle treats as terminal: a newer SDK
// against an older runtime cannot be fixed by reconnecting.
const (
	// ProtocolVersion is the wire contract revision these stubs were
	// generated against (sdk/proto).
	ProtocolVersion uint32 = 1
	// SDKLanguage names this SDK to the runtime and the console.
	SDKLanguage = "go"
	// SDKVersion is the module version. Bumped together with the release tag.
	SDKVersion = "0.2.0"
)
