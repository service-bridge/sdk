// ids.ts — monotonic UUID v7 generator (ADR-0006).
// @public — см. ./README.md

import { uuidv7 } from "uuidv7";

// uuidv7 generates a monotonic UUID v7. The `uuidv7` package guarantees the
// same clock-skew property ADR-0006 required (sequential ids within the same
// ms via an internal counter), running on plain Node (node:crypto) and Bun
// alike. Re-exported here so call sites depend on a single SDK entry point.
// @public — см. ./README.md
export { uuidv7 };
