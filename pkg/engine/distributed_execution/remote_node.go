package distributed_execution

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

type NodeType = logicalplan.NodeType
type Node = logicalplan.Node

// (to verify interface implementations)
var _ logicalplan.Node = (*Remote)(nil)
var _ logicalplan.UserDefinedExpr = (*Remote)(nil)

type Remote struct {
	Op   parser.ItemType
	Expr Node `json:"-"`
}

func NewRemoteNode() Node {
	return &Remote{}
}

func (p *Remote) Clone() Node {
	return &Remote{Op: p.Op, Expr: p.Expr.Clone()}
}
func (p *Remote) Children() []*Node {
	return []*Node{&p.Expr}
}
func (p *Remote) String() string {
	return fmt.Sprintf("%s%s", p.Op.String(), p.Expr.String())
}
func (p *Remote) ReturnType() parser.ValueType {
	return p.Expr.ReturnType()
}
func (p *Remote) Type() NodeType {
	return logicalplan.RemoteExecutionNode
}

func (p *Remote) MakeExecutionOperator(
	ctx context.Context,
	vectors *model.VectorPool,
	opts *query.Options,
	hints storage.SelectHints,
) (model.VectorOperator, error) {
	return newDistributedRemoteExecution(), nil
}

type RemoteExecution interface {
	Next()
	Series()
}

type DistributedRemoteExecution struct {
	RemoteExecution
}

func newDistributedRemoteExecution() *DistributedRemoteExecution {
	return &DistributedRemoteExecution{}
}

func (d DistributedRemoteExecution) Next(ctx context.Context) ([]model.StepVector, error) {
	//TODO
	return []model.StepVector{}, nil
}

func (d DistributedRemoteExecution) Series(ctx context.Context) ([]labels.Labels, error) {
	//TODO
	return []labels.Labels{}, nil
}

func (d DistributedRemoteExecution) GetPool() *model.VectorPool {
	//TODO
	return &model.VectorPool{}
}

func (d DistributedRemoteExecution) Explain() (next []model.VectorOperator) {
	//TODO
	return []model.VectorOperator{}
}

func (d DistributedRemoteExecution) String() string {
	//TODO implement
	return "distributed remote execution"
}
