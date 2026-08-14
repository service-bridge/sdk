package workflow

import (
	wf "github.com/service-bridge/sdk/go/workflow"
)

// CallTarget is one method a graph reaches out to under a name written in the
// graph itself.
type CallTarget struct {
	StepID  string
	Service string
	Method  string
}

// StaticCallTargets lists every method the graph calls under a literal name,
// forward steps and compensations alike, in declaration order.
//
// A target written as a Path is absent from the result: its name does not exist
// until the step runs against the state of a particular run, so nothing about it
// is decidable while the graph is being declared.
func StaticCallTargets(steps []wf.Step) []CallTarget {
	var out []CallTarget
	for _, step := range flattenSteps(steps, nil) {
		common := step.Common()
		call, isCall := step.(wf.Call)
		if isCall {
			if t, ok := staticTarget(common.ID, call.Service, call.Method); ok {
				out = append(out, t)
			}
		}
		comp := common.Compensate
		if comp == nil {
			continue
		}
		kind := string(comp.Kind)
		if kind == "" {
			kind = step.Kind()
		}
		if kind != wf.KindCall {
			continue
		}
		service, method := comp.Service, comp.Method
		if isCall {
			if service == nil {
				service = call.Service
			}
			if method == nil {
				method = call.Method
			}
		}
		if t, ok := staticTarget(common.ID+compensationSuffix, service, method); ok {
			out = append(out, t)
		}
	}
	return out
}

func staticTarget(stepID string, service, method wf.Target) (CallTarget, bool) {
	serviceName, ok := service.(wf.Name)
	if !ok {
		return CallTarget{}, false
	}
	methodName, ok := method.(wf.Name)
	if !ok {
		return CallTarget{}, false
	}
	return CallTarget{StepID: stepID, Service: string(serviceName), Method: string(methodName)}, true
}
