package distributed_execution

import (
	"context"
	"fmt"
	"github.com/cortexproject/cortex/pkg/ring/client"
	"io"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/promql-engine/execution/exchange"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	"github.com/cortexproject/cortex/pkg/engine/distributed_execution/querierpb"
)

type NodeType = logicalplan.NodeType
type Node = logicalplan.Node

const (
	RemoteNode = "RemoteNode"
)

// (to verify interface implementations)
var _ logicalplan.Node = (*Remote)(nil)
var _ logicalplan.UserDefinedExpr = (*Remote)(nil)

type Remote struct {
	Op   parser.ItemType
	Expr Node `json:"-"`

	Address string
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
func (p *Remote) Type() NodeType { return RemoteNode }

type poolKey struct{}

func ContextWithPool(ctx context.Context, pool *client.Pool) context.Context {
	return context.WithValue(ctx, poolKey{}, pool)
}

func PoolFromContext(ctx context.Context) *client.Pool {
	if pool, ok := ctx.Value(poolKey{}).(*client.Pool); ok {
		return pool
	}
	return nil
}

func (p *Remote) MakeExecutionOperator(
	ctx context.Context,
	vectors *model.VectorPool,
	opts *query.Options,
	hints storage.SelectHints,
) (model.VectorOperator, error) {
	pool := PoolFromContext(ctx)
	if pool == nil {
		return nil, fmt.Errorf("client pool not found in context")
	}

	remoteExec, err := newDistributedRemoteExecution(p.Address, pool)
	if err != nil {
		return nil, err
	}
	return exchange.NewConcurrent(remoteExec, 2, opts), nil
}

type DistributedRemoteExecution struct {
	client     querierpb.QuerierClient
	fragmentID uint64
	batchSize  int64
	series     []labels.Labels
	addr       string
}

func newDistributedRemoteExecution(addr string, pool *client.Pool) (*DistributedRemoteExecution, error) {
	poolClient, err := pool.GetClientFor(addr)
	if err != nil {
		return nil, err
	}

	client, ok := poolClient.(*querierClient)
	if !ok {
		return nil, fmt.Errorf("invalid client type from pool")
	}

	return &DistributedRemoteExecution{
		client:    client,
		addr:      addr,
		batchSize: 1000,
	}, nil
}

func (d *DistributedRemoteExecution) Series(ctx context.Context) ([]labels.Labels, error) {
	if d.series != nil {
		return d.series, nil
	}

	req := &querierpb.SeriesRequest{
		FragmentID: d.fragmentID,
		Batchsize:  d.batchSize,
	}

	stream, err := d.client.Series(ctx, req)
	if err != nil {
		return nil, err
	}

	var series []labels.Labels
	for {
		oneSeries, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		lbs := make(labels.Labels, len(oneSeries.Labels))
		for i, l := range oneSeries.Labels {
			lbs[i] = labels.Label{Name: l.Name, Value: l.Value}
		}
		series = append(series, lbs)
	}

	d.series = series
	return series, nil
}

func (d *DistributedRemoteExecution) Next(ctx context.Context) ([]model.StepVector, error) {
	req := &querierpb.NextRequest{
		FragmentID: d.fragmentID,
		Batchsize:  d.batchSize,
	}

	stream, err := d.client.Next(ctx, req)
	if err != nil {
		return nil, err
	}

	batch, err := stream.Recv()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	result := make([]model.StepVector, len(batch.StepVectors))
	for i, sv := range batch.StepVectors {
		result[i] = model.StepVector{
			T:            sv.T,
			SampleIDs:    sv.Sample_IDs,
			Samples:      sv.Samples,
			HistogramIDs: sv.Histogram_IDs,
			Histograms:   FloatHistogramProtoToFloatHistograms(sv.Histograms),
		}
	}
	return result, nil
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
