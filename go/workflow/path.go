package workflow

// Path is a JSONPath-lite expression read from workflow run state at the moment
// a step executes. It is a type of its own rather than a string so a literal
// that happens to look like a path is never mistaken for one: the Node SDK has
// to wrap such a literal in an escape object, Go tells the two apart by type.
//
// The accepted grammar is `$` followed by any number of `.field`, `[N]` and
// `[*]` segments. `[*]` followed by `.field` collects that field from every
// element into an array.
type Path string

func (Path) isTarget() {}

// Target names what a step acts on. It is either a literal Name or a Path
// resolved against run state, and nothing else: the marker method is
// unexported, so the set is closed at the package boundary.
type Target interface {
	isTarget()
}

// Name is a target written out at declaration time.
type Name string

func (Name) isTarget() {}

// Predicate is the condition under which a step runs. The set is closed — build
// one with Truthy, Not, Equals, In, And or Or.
type Predicate interface {
	// Node is the predicate as it travels in the graph: the same value the
	// fingerprint covers and the runner evaluates.
	Node() any
	isPredicate()
}

// Truthy holds when the value at p is present and not false, zero or empty.
func Truthy(p Path) Predicate { return truthy{path: p} }

// Not inverts p.
func Not(p Predicate) Predicate { return negation{inner: p} }

// Equals holds when both values resolve to the same JSON value. Either side may
// be a Path, a literal, or a tree containing both.
func Equals(left, right any) Predicate { return equality{left: left, right: right} }

// In holds when value resolves to an element of list.
func In(value, list any) Predicate { return membership{value: value, list: list} }

// And holds when every predicate holds.
func And(preds ...Predicate) Predicate { return conjunction{preds: preds} }

// Or holds when at least one predicate holds.
func Or(preds ...Predicate) Predicate { return disjunction{preds: preds} }

type truthy struct{ path Path }

func (t truthy) Node() any  { return t.path }
func (truthy) isPredicate() {}

type negation struct{ inner Predicate }

func (n negation) Node() any  { return map[string]any{"not": predicateNode(n.inner)} }
func (negation) isPredicate() {}

type equality struct{ left, right any }

func (e equality) Node() any  { return map[string]any{"equals": []any{e.left, e.right}} }
func (equality) isPredicate() {}

type membership struct{ value, list any }

func (m membership) Node() any  { return map[string]any{"in": []any{m.value, m.list}} }
func (membership) isPredicate() {}

type conjunction struct{ preds []Predicate }

func (c conjunction) Node() any  { return map[string]any{"and": predicateNodes(c.preds)} }
func (conjunction) isPredicate() {}

type disjunction struct{ preds []Predicate }

func (d disjunction) Node() any  { return map[string]any{"or": predicateNodes(d.preds)} }
func (disjunction) isPredicate() {}

// predicateNode keeps a missing operand as an explicit nil instead of panicking,
// so validation reports the malformed predicate at the declaration.
func predicateNode(p Predicate) any {
	if p == nil {
		return nil
	}
	return p.Node()
}

func predicateNodes(preds []Predicate) []any {
	out := make([]any, 0, len(preds))
	for _, p := range preds {
		out = append(out, predicateNode(p))
	}
	return out
}
