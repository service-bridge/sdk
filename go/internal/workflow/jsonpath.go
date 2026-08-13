// Package workflow freezes a declared workflow graph into the exact bytes the
// runtime registers it by, and resolves the expressions the graph carries
// against the state of a run.
package workflow

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	wf "github.com/service-bridge/sdk/go/workflow"
)

// Reasons an expression is refused. A missing value is never one of them: a
// step skipped by its condition leaves nothing behind, and every consumer has
// to survive reading it.
var (
	ErrPathSyntax     = errors.New("path expression is not parseable")
	ErrPathType       = errors.New("path expression applied to a value of the wrong shape")
	ErrGraphValue     = errors.New("value cannot travel in a workflow graph")
	ErrTargetShape    = errors.New("target must resolve to a string")
	ErrPredicateShape = errors.New("predicate is none of truthy, not, equals, in, and, or")
)

// PathError reports an expression the evaluator cannot use, naming the
// expression itself so the graph author sees which one.
type PathError struct {
	Expr   string
	Reason error
}

func (e *PathError) Error() string {
	return fmt.Sprintf("workflow: evaluate path %q: %s", e.Expr, e.Reason)
}

func (e *PathError) Unwrap() error { return e.Reason }

func pathError(expr wf.Path, reason error) error {
	return &PathError{Expr: string(expr), Reason: reason}
}

// pathToken is one segment of a parsed expression: a field, an index, or the
// wildcard that turns the cursor into an array.
type pathToken struct {
	field    string
	index    int
	indexed  bool
	wildcard bool
}

// ValidatePath reports whether expr is syntactically usable. It is the check
// run at declaration time, where a typo is cheap to fix.
func ValidatePath(expr wf.Path) error {
	_, err := parsePath(expr)
	return err
}

// EvalPath resolves expr against the state of a run. A path that leads nowhere
// resolves to nil; only a malformed expression, or a wildcard over something
// that is not an array, is an error.
func EvalPath(expr wf.Path, state map[string]any) (any, error) {
	tokens, err := parsePath(expr)
	if err != nil {
		return nil, err
	}

	var cursor any = state
	wildcard := false
	for _, tok := range tokens {
		switch {
		case tok.wildcard:
			if cursor == nil {
				cursor = []any{}
				wildcard = true
				continue
			}
			items, ok := asSlice(cursor)
			if !ok {
				return nil, pathError(expr, fmt.Errorf("%w: [*] over a non-array", ErrPathType))
			}
			cursor = items
			wildcard = true

		case tok.indexed:
			items, ok := asSlice(cursor)
			if ok && tok.index < len(items) {
				cursor = items[tok.index]
			} else {
				cursor = nil
			}
			// An index picks one element, so the following fields address that
			// element rather than mapping over the array.
			wildcard = false

		default:
			if wildcard {
				// The wildcard arm above is the only way wildcard becomes true,
				// and it always leaves a []any behind.
				items := cursor.([]any)
				mapped := make([]any, len(items))
				for i, item := range items {
					mapped[i] = fieldOf(item, tok.field)
				}
				cursor = mapped
				continue
			}
			cursor = fieldOf(cursor, tok.field)
		}
	}
	return cursor, nil
}

func parsePath(expr wf.Path) ([]pathToken, error) {
	raw := string(expr)
	if !strings.HasPrefix(raw, "$") {
		return nil, pathError(expr, fmt.Errorf("%w: must start with $", ErrPathSyntax))
	}

	rest := raw[1:]
	var tokens []pathToken
	for len(rest) > 0 {
		switch rest[0] {
		case '.':
			end := 1
			if end >= len(rest) || !isIdentStart(rest[end]) {
				return nil, pathError(expr, fmt.Errorf("%w: field name expected after '.'", ErrPathSyntax))
			}
			end++
			for end < len(rest) && isIdentPart(rest[end]) {
				end++
			}
			tokens = append(tokens, pathToken{field: rest[1:end]})
			rest = rest[end:]

		case '[':
			closing := strings.IndexByte(rest, ']')
			if closing < 0 {
				return nil, pathError(expr, fmt.Errorf("%w: unclosed '['", ErrPathSyntax))
			}
			inner := rest[1:closing]
			if inner == "*" {
				tokens = append(tokens, pathToken{wildcard: true})
			} else {
				if !isDigits(inner) {
					return nil, pathError(expr, fmt.Errorf("%w: index must be a non-negative integer or *", ErrPathSyntax))
				}
				idx, err := strconv.Atoi(inner)
				if err != nil {
					return nil, pathError(expr, fmt.Errorf("%w: %v", ErrPathSyntax, err))
				}
				tokens = append(tokens, pathToken{index: idx, indexed: true})
			}
			rest = rest[closing+1:]

		default:
			return nil, pathError(expr, fmt.Errorf("%w: unexpected %q", ErrPathSyntax, rest[0]))
		}
	}
	return tokens, nil
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// fieldOf reads one field. Anything that is not a string-keyed map has no
// fields, and reading one yields nothing rather than failing.
func fieldOf(v any, name string) any {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]any); ok {
		return m[name]
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String {
		return nil
	}
	got := rv.MapIndex(reflect.ValueOf(name).Convert(rv.Type().Key()))
	if !got.IsValid() {
		return nil
	}
	return got.Interface()
}

func asSlice(v any) ([]any, bool) {
	if v == nil {
		return nil, false
	}
	if items, ok := v.([]any); ok {
		return items, true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]any, rv.Len())
	for i := range out {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}

// EvalValue resolves a declared value against run state: every Path inside it
// becomes what the path points at, everything else stays as written.
func EvalValue(v any, state map[string]any) (any, error) {
	return walkValue(v,
		func(p wf.Path) (any, error) { return EvalPath(p, state) },
		func(s string) (any, error) { return s, nil },
	)
}

// EvalTarget resolves what a step acts on. A Name is already the answer; a Path
// has to lead to a string, because a service, a method and an event are names.
func EvalTarget(t wf.Target, state map[string]any) (string, error) {
	switch target := t.(type) {
	case nil:
		return "", fmt.Errorf("workflow: resolve target: %w: target is absent", ErrTargetShape)
	case wf.Name:
		return string(target), nil
	case wf.Path:
		got, err := EvalPath(target, state)
		if err != nil {
			return "", fmt.Errorf("workflow: resolve target: %w", err)
		}
		name, ok := got.(string)
		if !ok {
			return "", fmt.Errorf("workflow: resolve target %q: %w: got %T", string(target), ErrTargetShape, got)
		}
		return name, nil
	default:
		return "", fmt.Errorf("workflow: resolve target: %w: unknown target %T", ErrTargetShape, t)
	}
}

// EvalPredicate decides whether a step runs. A step without a condition always
// runs, so a nil predicate holds.
func EvalPredicate(p wf.Predicate, state map[string]any) (bool, error) {
	if p == nil {
		return true, nil
	}
	return evalPredicateNode(p.Node(), state)
}

func evalPredicateNode(node any, state map[string]any) (bool, error) {
	switch n := node.(type) {
	case wf.Path:
		got, err := EvalPath(n, state)
		if err != nil {
			return false, err
		}
		return isTruthy(got), nil

	case map[string]any:
		if len(n) != 1 {
			return false, fmt.Errorf("workflow: evaluate predicate: %w", ErrPredicateShape)
		}
		for op, operand := range n {
			return evalPredicateOp(op, operand, state)
		}
	}
	return false, fmt.Errorf("workflow: evaluate predicate: %w", ErrPredicateShape)
}

func evalPredicateOp(op string, operand any, state map[string]any) (bool, error) {
	switch op {
	case "not":
		inner, err := evalPredicateNode(operand, state)
		if err != nil {
			return false, err
		}
		return !inner, nil

	case "equals":
		left, right, err := predicatePair(operand, state)
		if err != nil {
			return false, err
		}
		return jsonEqual(left, right), nil

	case "in":
		value, list, err := predicatePair(operand, state)
		if err != nil {
			return false, err
		}
		items, ok := asSlice(list)
		if !ok {
			return false, nil
		}
		for _, item := range items {
			if jsonEqual(item, value) {
				return true, nil
			}
		}
		return false, nil

	case "and", "or":
		nodes, ok := operand.([]any)
		if !ok || len(nodes) == 0 {
			return false, fmt.Errorf("workflow: evaluate predicate %q: %w", op, ErrPredicateShape)
		}
		for _, sub := range nodes {
			held, err := evalPredicateNode(sub, state)
			if err != nil {
				return false, err
			}
			if op == "and" && !held {
				return false, nil
			}
			if op == "or" && held {
				return true, nil
			}
		}
		return op == "and", nil

	default:
		return false, fmt.Errorf("workflow: evaluate predicate %q: %w", op, ErrPredicateShape)
	}
}

func predicatePair(operand any, state map[string]any) (any, any, error) {
	pair, ok := operand.([]any)
	if !ok || len(pair) != 2 {
		return nil, nil, fmt.Errorf("workflow: evaluate predicate: %w: expected two operands", ErrPredicateShape)
	}
	left, err := EvalValue(pair[0], state)
	if err != nil {
		return nil, nil, err
	}
	right, err := EvalValue(pair[1], state)
	if err != nil {
		return nil, nil, err
	}
	return left, right, nil
}

func isTruthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	}
	if n, ok := asNumber(v); ok {
		return n != 0
	}
	return true
}

func asNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	}
	return 0, false
}

// jsonEqual compares two resolved values the way JSON sees them: a number read
// out of decoded state and the same number written in the graph are equal even
// though Go types them differently.
func jsonEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if na, ok := asNumber(a); ok {
		nb, ok := asNumber(b)
		return ok && na == nb
	}
	if sa, ok := a.(string); ok {
		sb, ok := b.(string)
		return ok && sa == sb
	}
	if ba, ok := a.(bool); ok {
		bb, ok := b.(bool)
		return ok && ba == bb
	}
	if items, ok := asSlice(a); ok {
		other, ok := asSlice(b)
		if !ok || len(items) != len(other) {
			return false
		}
		for i := range items {
			if !jsonEqual(items[i], other[i]) {
				return false
			}
		}
		return true
	}
	ma, okA := asStringMap(a)
	mb, okB := asStringMap(b)
	if okA && okB {
		if len(ma) != len(mb) {
			return false
		}
		for k, v := range ma {
			other, found := mb[k]
			if !found || !jsonEqual(v, other) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(a, b)
}

func asStringMap(v any) (map[string]any, bool) {
	if m, ok := v.(map[string]any); ok {
		return m, true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	out := make(map[string]any, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		out[iter.Key().String()] = iter.Value().Interface()
	}
	return out, true
}

// walkValue rebuilds a declared value tree, handing every leaf to one of the
// two callbacks. Canonicalisation and evaluation are the same walk over the
// same tree and differ only in what a leaf becomes, so they share it.
//
// A value that is neither a container nor a scalar — a struct, a json.RawMessage
// — is opaque: it travels as written and carries no expressions, because a Path
// buried in a Go struct field is indistinguishable from a string once encoded.
func walkValue(v any, onPath func(wf.Path) (any, error), onText func(string) (any, error)) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case wf.Path:
		return onPath(t)
	case wf.Name:
		return onText(string(t))
	case string:
		return onText(t)
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return t, nil
	case float32:
		return finiteFloat(float64(t))
	case float64:
		return finiteFloat(t)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, item := range t {
			walked, err := walkValue(item, onPath, onText)
			if err != nil {
				return nil, err
			}
			out[k] = walked
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			walked, err := walkValue(item, onPath, onText)
			if err != nil {
				return nil, err
			}
			out[i] = walked
		}
		return out, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil, nil
		}
		return walkValue(rv.Elem().Interface(), onPath, onText)

	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("workflow: walk value: %w: map keyed by %s", ErrGraphValue, rv.Type().Key())
		}
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			walked, err := walkValue(iter.Value().Interface(), onPath, onText)
			if err != nil {
				return nil, err
			}
			out[iter.Key().String()] = walked
		}
		return out, nil

	case reflect.Slice, reflect.Array:
		// Byte slices are JSON strings, not arrays of numbers; walking them
		// element by element would change what they encode to.
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return v, nil
		}
		out := make([]any, rv.Len())
		for i := range out {
			walked, err := walkValue(rv.Index(i).Interface(), onPath, onText)
			if err != nil {
				return nil, err
			}
			out[i] = walked
		}
		return out, nil

	case reflect.String:
		return onText(rv.String())

	case reflect.Bool:
		return rv.Bool(), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint(), nil

	case reflect.Float32, reflect.Float64:
		return finiteFloat(rv.Float())

	case reflect.Chan, reflect.Func, reflect.UnsafePointer, reflect.Complex64, reflect.Complex128:
		return nil, fmt.Errorf("workflow: walk value: %w: %s", ErrGraphValue, rv.Kind())
	}

	return v, nil
}

func finiteFloat(f float64) (any, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("workflow: walk value: %w: %v has no JSON form", ErrGraphValue, f)
	}
	return f, nil
}
