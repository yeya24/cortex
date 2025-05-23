package stats

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic" //lint:ignore faillint we can't use go.uber.org/atomic with a protobuf struct without wrapping it.
	"time"

	"github.com/weaveworks/common/httpgrpc"
)

type contextKey int

var ctxKey = contextKey(0)

type QueryStats struct {
	Stats
	QueryResponseSeries uint64
	PriorityAssigned    bool
	Priority            int64
	DataSelectMaxTime   int64
	DataSelectMinTime   int64
	SplitInterval       time.Duration
	m                   sync.Mutex
}

// ContextWithEmptyStats returns a context with empty stats.
func ContextWithEmptyStats(ctx context.Context) (*QueryStats, context.Context) {
	stats := &QueryStats{}
	ctx = context.WithValue(ctx, ctxKey, stats)
	return stats, ctx
}

// FromContext gets the Stats out of the Context. Returns nil if stats have not
// been initialised in the context.
func FromContext(ctx context.Context) *QueryStats {
	o := ctx.Value(ctxKey)
	if o == nil {
		return nil
	}
	return o.(*QueryStats)
}

// IsEnabled returns whether stats tracking is enabled in the context.
func IsEnabled(ctx context.Context) bool {
	// When query statistics are enabled, the stats object is already initialised
	// within the context, so we can just check it.
	return FromContext(ctx) != nil
}

func (s *QueryStats) Copy() *QueryStats {
	if s == nil {
		return nil
	}

	copied := &QueryStats{}
	copied.Merge(s)
	return copied
}

// AddWallTime adds some time to the counter.
func (s *QueryStats) AddWallTime(t time.Duration) {
	if s == nil {
		return
	}

	atomic.AddInt64((*int64)(&s.WallTime), int64(t))
}

// LoadWallTime returns current wall time.
func (s *QueryStats) LoadWallTime() time.Duration {
	if s == nil {
		return 0
	}

	return time.Duration(atomic.LoadInt64((*int64)(&s.WallTime)))
}

func (s *QueryStats) AddResponseSeries(series uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.QueryResponseSeries, series)
}

func (s *QueryStats) LoadResponseSeries() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.QueryResponseSeries)
}

func (s *QueryStats) AddFetchedSeries(series uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedSeriesCount, series)
}

func (s *QueryStats) AddExtraFields(fieldsVals ...interface{}) {
	if s == nil {
		return
	}

	s.m.Lock()
	defer s.m.Unlock()

	if s.ExtraFields == nil {
		s.ExtraFields = map[string]string{}
	}

	if len(fieldsVals)%2 == 1 {
		fieldsVals = append(fieldsVals, "")
	}

	for i := 0; i < len(fieldsVals); i += 2 {
		if v, ok := fieldsVals[i].(string); ok {
			s.ExtraFields[v] = fmt.Sprintf("%v", fieldsVals[i+1])
		}
	}
}

func (s *QueryStats) LoadExtraFields() []interface{} {
	if s == nil {
		return []interface{}{}
	}

	s.m.Lock()
	defer s.m.Unlock()

	r := make([]interface{}, 0, len(s.ExtraFields))
	for k, v := range s.ExtraFields {
		r = append(r, k, v)
	}

	return r
}

func (s *QueryStats) LoadFetchedSeries() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedSeriesCount)
}

func (s *QueryStats) AddFetchedChunkBytes(bytes uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedChunkBytes, bytes)
}

func (s *QueryStats) LoadFetchedChunkBytes() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedChunkBytes)
}

func (s *QueryStats) AddFetchedDataBytes(bytes uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedDataBytes, bytes)
}

func (s *QueryStats) LoadFetchedDataBytes() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedDataBytes)
}

func (s *QueryStats) AddFetchedSamples(count uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedSamplesCount, count)
}

func (s *QueryStats) LoadFetchedSamples() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedSamplesCount)
}

func (s *QueryStats) AddFetchedChunks(count uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedChunksCount, count)
}

func (s *QueryStats) LoadFetchedChunks() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedChunksCount)
}

// AddQueryStorageWallTime adds some time to the counter.
func (s *QueryStats) AddQueryStorageWallTime(t time.Duration) {
	if s == nil {
		return
	}

	atomic.AddInt64((*int64)(&s.QueryStorageWallTime), int64(t))
}

// LoadQueryStorageWallTime returns current query storage wall time.
func (s *QueryStats) LoadQueryStorageWallTime() time.Duration {
	if s == nil {
		return 0
	}

	return time.Duration(atomic.LoadInt64((*int64)(&s.QueryStorageWallTime)))
}

func (s *QueryStats) AddSplitQueries(count uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.SplitQueries, count)
}

func (s *QueryStats) LoadSplitQueries() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.SplitQueries)
}

func (s *QueryStats) SetPriority(priority int64) {
	if s == nil {
		return
	}

	if !s.PriorityAssigned {
		s.PriorityAssigned = true
	}

	atomic.StoreInt64(&s.Priority, priority)
}

func (s *QueryStats) LoadPriority() (int64, bool) {
	if s == nil {
		return 0, false
	}

	return atomic.LoadInt64(&s.Priority), s.PriorityAssigned
}

func (s *QueryStats) SetDataSelectMaxTime(dataSelectMaxTime int64) {
	if s == nil {
		return
	}

	atomic.StoreInt64(&s.DataSelectMaxTime, dataSelectMaxTime)
}

func (s *QueryStats) LoadDataSelectMaxTime() int64 {
	if s == nil {
		return 0
	}

	return atomic.LoadInt64(&s.DataSelectMaxTime)
}

func (s *QueryStats) SetDataSelectMinTime(dataSelectMinTime int64) {
	if s == nil {
		return
	}

	atomic.StoreInt64(&s.DataSelectMinTime, dataSelectMinTime)
}

func (s *QueryStats) LoadDataSelectMinTime() int64 {
	if s == nil {
		return 0
	}

	return atomic.LoadInt64(&s.DataSelectMinTime)
}

func (s *QueryStats) LoadSplitInterval() time.Duration {
	if s == nil {
		return 0
	}

	return s.SplitInterval
}

func (s *QueryStats) AddStoreGatewayTouchedPostings(count uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.StoreGatewayTouchedPostingsCount, count)
}

func (s *QueryStats) LoadStoreGatewayTouchedPostings() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.StoreGatewayTouchedPostingsCount)
}

func (s *QueryStats) AddStoreGatewayTouchedPostingBytes(bytes uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.StoreGatewayTouchedPostingBytes, bytes)
}

func (s *QueryStats) LoadStoreGatewayTouchedPostingBytes() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.StoreGatewayTouchedPostingBytes)
}

func (s *QueryStats) AddScannedSamples(count uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.ScannedSamples, count)
}

func (s *QueryStats) LoadScannedSamples() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.ScannedSamples)
}

func (s *QueryStats) AddPeakSamples(count uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.PeakSamples, count)
}

func (s *QueryStats) SetPeakSamples(count uint64) {
	if s == nil {
		return
	}

	atomic.StoreUint64(&s.PeakSamples, count)
}

func (s *QueryStats) LoadPeakSamples() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.PeakSamples)
}

// Merge the provided Stats into this one.
func (s *QueryStats) Merge(other *QueryStats) {
	if s == nil || other == nil {
		return
	}

	s.AddWallTime(other.LoadWallTime())
	s.AddQueryStorageWallTime(other.LoadQueryStorageWallTime())
	s.AddFetchedSeries(other.LoadFetchedSeries())
	s.AddFetchedChunkBytes(other.LoadFetchedChunkBytes())
	s.AddFetchedDataBytes(other.LoadFetchedDataBytes())
	s.AddFetchedSamples(other.LoadFetchedSamples())
	s.AddFetchedChunks(other.LoadFetchedChunks())
	s.AddStoreGatewayTouchedPostings(other.LoadStoreGatewayTouchedPostings())
	s.AddStoreGatewayTouchedPostingBytes(other.LoadStoreGatewayTouchedPostingBytes())
	s.AddScannedSamples(other.LoadScannedSamples())
	s.SetPeakSamples(max(s.LoadPeakSamples(), other.LoadPeakSamples()))
	s.AddExtraFields(other.LoadExtraFields()...)
}

func ShouldTrackHTTPGRPCResponse(r *httpgrpc.HTTPResponse) bool {
	// Do no track statistics for requests failed because of a server error.
	return r.Code < 500
}
