package distributed_execution

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/promql-engine/execution/exchange"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	"github.com/cortexproject/cortex/pkg/engine/distributed_execution/querierpb"
	"github.com/cortexproject/cortex/pkg/ring/client"
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

	FragmentKey  FragmentKey
	FragmentAddr string
}

func NewRemoteNode() Node {
	return &Remote{
		// initialize the fragment key pointer first
		FragmentKey: FragmentKey{},
	}
}
func (r *Remote) Clone() Node {
	return &Remote{Op: r.Op, Expr: r.Expr.Clone(), FragmentKey: r.FragmentKey}
}
func (r *Remote) Children() []*Node {
	return []*Node{&r.Expr}
}
func (r *Remote) String() string {
	return fmt.Sprintf("%s%s", r.Op.String(), r.Expr.String())
}
func (r *Remote) ReturnType() parser.ValueType {
	return r.Expr.ReturnType()
}
func (r *Remote) Type() NodeType { return RemoteNode }

type remote struct {
	QueryID      uint64
	FragmentID   uint64
	FragmentAddr string
}

func (r *Remote) MarshalJSON() ([]byte, error) {
	return json.Marshal(remote{
		QueryID:      r.FragmentKey.queryID,
		FragmentID:   r.FragmentKey.fragmentID,
		FragmentAddr: r.FragmentAddr,
	})
}

func (r *Remote) UnmarshalJSON(data []byte) error {
	re := remote{}
	if err := json.Unmarshal(data, &re); err != nil {
		return err
	}

	r.FragmentKey = *MakeFragmentKey(re.QueryID, re.FragmentID)
	r.FragmentAddr = re.FragmentAddr
	return nil
}

type poolKey struct{}

// TODO: change to using an extra layer to put the querier address in it
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

	remoteExec, err := newDistributedRemoteExecution(ctx, pool, p.FragmentKey)
	if err != nil {
		return nil, err
	}
	return exchange.NewConcurrent(remoteExec, 2, opts), nil
}

type DistributedRemoteExecution struct {
	client    querierpb.QuerierClient
	batchSize int64
	series    []labels.Labels

	fragmentKey FragmentKey
	addr        string
}

type QuerierAddrKey struct{}

type ChildFragmentKey struct{}

func newDistributedRemoteExecution(ctx context.Context, pool *client.Pool, fragmentKey FragmentKey) (*DistributedRemoteExecution, error) {

	_, _, _, childIDToAddr, _ := ExtractFragmentMetaData(ctx)

	poolClient, err := pool.GetClientFor(childIDToAddr[fragmentKey.fragmentID])

	if err != nil {
		return nil, err
	}

	client, ok := poolClient.(*querierClient)
	if !ok {
		return nil, fmt.Errorf("invalid client type from pool")
	}

	return &DistributedRemoteExecution{
		client:      client,
		fragmentKey: fragmentKey,
		addr:        childIDToAddr[fragmentKey.fragmentID],
		batchSize:   1000, // TODO: make it a config param?
	}, nil
}

func (d *DistributedRemoteExecution) Series(ctx context.Context) ([]labels.Labels, error) {
	if d.series != nil {
		return d.series, nil
	}

	req := &querierpb.SeriesRequest{
		QueryID:    d.fragmentKey.queryID,
		FragmentID: d.fragmentKey.fragmentID,
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
		QueryID:    d.fragmentKey.queryID,
		FragmentID: d.fragmentKey.fragmentID,
		Batchsize:  d.batchSize,
	}

	//stream, err := d.client.Next(ctx, req)
	//if err != nil {
	//	return nil, err
	//}
	//
	//batch, err := stream.Recv()
	//if err == io.EOF {
	//	break
	//}
	//if err != nil {
	//	return nil, err
	//}
	//
	//result := make([]model.StepVector, len(batch.StepVectors))
	//
	//for i, sv := range batch.StepVectors {
	//	result[i] = model.StepVector{
	//		T:            sv.T,
	//		SampleIDs:    sv.Sample_IDs,
	//		Samples:      sv.Samples,
	//		HistogramIDs: sv.Histogram_IDs,
	//		Histograms:   FloatHistogramProtoToFloatHistograms(sv.Histograms),
	//	}
	//}

	stream, err := d.client.Next(ctx, req)
	if err != nil {
		return nil, err
	}

	var result []model.StepVector
	for {
		stepVectorBatch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		for i, sv := range stepVectorBatch.StepVectors {
			result[i] = model.StepVector{
				T:            sv.T,
				SampleIDs:    sv.Sample_IDs,
				Samples:      sv.Samples,
				HistogramIDs: sv.Histogram_IDs,
				Histograms:   FloatHistogramProtoToFloatHistograms(sv.Histograms)}
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
