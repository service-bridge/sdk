//go:build e2e && runtime_next

package e2e

// The runtime_next build includes the conformance scenarios that need a
// runtime routing deliveries by matched_patterns and evaluating filters.
func init() { runtimeNext = true }
