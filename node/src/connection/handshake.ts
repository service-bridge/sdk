// Handshake identity the SDK sends in OpenRequest and RegisterRequest. The
// runtime refuses a protocol revision it does not speak with
// FAILED_PRECONDITION, which the connection lifecycle treats as terminal: a
// newer SDK against an older runtime cannot be fixed by reconnecting.
//
// @internal — см. ./README.md

/** Wire contract revision these stubs were generated against (sdk/proto). */
export const PROTOCOL_VERSION = 1;

/** Names this SDK to the runtime and the console. */
export const SDK_LANGUAGE = "node";

/** npm package version; handshake.test.ts keeps it equal to package.json. */
export const SDK_VERSION = "2.0.0-alpha.16";
