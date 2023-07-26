package tripperware

import (
	"net/http"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"
)

var (
	ErrSubQueryStepTooSmall       = "exceeded maximum resolution of %d points per timeseries in subquery, got %d steps. Try increasing the step size of your subquery"
	ErrSubQueryTotalStepsExceeded = "exceeded total allowed number of subquery steps %d in the query, got %d steps."
)

const (
	MaxStep = 11000
)

// SubQueryStepSizeCheck ensures the query doesn't contain too small step size and too many total steps in subqueries.
func SubQueryStepSizeCheck(query string, defaultSubQueryInterval time.Duration, maxStep, maxTotalSteps int) error {
	totalSteps, maxSteps := GetSubQueryStepsFromQuery(query, defaultSubQueryInterval)
	if maxSteps > maxStep {
		return httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryStepTooSmall, maxStep, maxSteps)
	}
	if totalSteps > maxTotalSteps {
		return httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryTotalStepsExceeded, maxTotalSteps, totalSteps)
	}
	return nil
}

// GetSubQueryStepsFromQuery returns total steps of subqueries for the given query.
func GetSubQueryStepsFromQuery(query string, defaultSubQueryInterval time.Duration) (int, int) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		// If query fails to parse, we don't throw step size error
		// but fail query later on querier.
		return 0, 0
	}
	var maxSteps int
	totalSteps := traverseBottomUp(&expr, func(node *parser.Expr) int {
		e, ok := (*node).(*parser.SubqueryExpr)
		if ok {
			step := e.Step
			if e.Step == 0 {
				step = defaultSubQueryInterval
			}
			iters := int(e.Range / step)
			if iters > maxSteps {
				maxSteps = iters
			}
			return iters
		}
		return 0
	})
	return totalSteps, maxSteps
}

// Traverse the syntax tree from bottom up and calculate total subquery steps.
func traverseBottomUp(current *parser.Expr, transform func(node *parser.Expr) int) int {
	switch node := (*current).(type) {
	case *parser.BinaryExpr:
		iterLeft := traverseBottomUp(&node.LHS, transform)
		iterRight := traverseBottomUp(&node.RHS, transform)
		return iterLeft + iterRight
	case *parser.AggregateExpr:
		return traverseBottomUp(&node.Expr, transform)
	case *parser.Call:
		iters := 0
		for i := range node.Args {
			iters += traverseBottomUp(&node.Args[i], transform)
		}
		return iters
	case *parser.StepInvariantExpr:
		return traverseBottomUp(&node.Expr, transform)
	case *parser.SubqueryExpr:
		inner := traverseBottomUp(&node.Expr, transform)
		cur := transform(current)
		if inner == 0 {
			return cur
		}
		return inner * cur
	case *parser.ParenExpr:
		return traverseBottomUp(&node.Expr, transform)
	case *parser.UnaryExpr:
		return traverseBottomUp(&node.Expr, transform)
	default:
		return 0
	}
}
