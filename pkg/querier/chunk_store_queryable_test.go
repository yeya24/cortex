package querier

import (
	"context"
	"fmt"
	"github.com/cortexproject/cortex/pkg/querier/batch"
	"github.com/cortexproject/cortex/pkg/querier/iterators"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
)

// Make sure that chunkSeries implements SeriesWithChunks
var _ SeriesWithChunks = &chunkSeries{}

func TestChunkQueryable(t *testing.T) {
	t.Parallel()
	opts := promql.EngineOpts{
		Logger:     log.NewNopLogger(),
		MaxSamples: 1e6,
		Timeout:    1 * time.Minute,
	}
	for _, thanosEngine := range []bool{false, true} {
		for _, testcase := range testcases {
			for _, encoding := range encodings {
				for _, query := range queries {
					t.Run(fmt.Sprintf("%s/%s/%s/ thanos engine enabled = %t", testcase.name, encoding.name, query.query, thanosEngine), func(t *testing.T) {
						var queryEngine promql.QueryEngine
						if thanosEngine {
							queryEngine = engine.New(engine.Opts{
								EngineOpts:        opts,
								LogicalOptimizers: logicalplan.AllOptimizers,
							})
						} else {
							queryEngine = promql.NewEngine(opts)
						}

						store, from := makeMockChunkStore(t, 24, encoding.e)
						queryable := newMockStoreQueryable(store, testcase.f)
						testRangeQuery(t, queryable, queryEngine, from, query)
					})
				}
			}
		}
	}
}

type mockChunkStore struct {
	chunks []chunk.Chunk
}

func (m mockChunkStore) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {
	return m.chunks, nil
}

func makeMockChunkStore(t require.TestingT, numChunks int, encoding promchunk.Encoding) (mockChunkStore, model.Time) {
	var (
		chunks = make([]chunk.Chunk, 0, numChunks)
		from   = model.Time(0)
	)
	for i := 0; i < numChunks; i++ {
		c := mkChunk(t, from, from.Add(samplesPerChunk*sampleRate), sampleRate, encoding)
		chunks = append(chunks, c)
		from = from.Add(chunkOffset)
	}
	return mockChunkStore{chunks}, from
}

func mkChunk(t require.TestingT, mint, maxt model.Time, step time.Duration, encoding promchunk.Encoding) chunk.Chunk {
	metric := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
	}
	pc, err := promchunk.NewForEncoding(encoding)
	require.NoError(t, err)
	for i := mint; i.Before(maxt); i = i.Add(step) {
		nc, err := pc.Add(model.SamplePair{
			Timestamp: i,
			Value:     model.SampleValue(float64(i)),
		})
		require.NoError(t, err)
		require.Nil(t, nc)
	}
	return chunk.NewChunk(metric, pc, mint, maxt)
}

func TestPartitionChunksOutputIsSortedByLabels(t *testing.T) {
	t.Parallel()

	var allChunks []chunk.Chunk

	const count = 10
	// go down, to add series in reversed order
	for i := count; i > 0; i-- {
		ch := mkChunk(t, model.Time(0), model.Time(1000), time.Millisecond, promchunk.PrometheusXorChunk)
		// mkChunk uses `foo` as metric name, so we rename metric to be unique
		ch.Metric[0].Value = fmt.Sprintf("%02d", i)

		allChunks = append(allChunks, ch)
	}

	res := partitionChunks(allChunks, 0, 1000, mergeChunks)

	// collect labels from each series
	var seriesLabels []labels.Labels
	for res.Next() {
		seriesLabels = append(seriesLabels, res.At().Labels())
	}

	require.Len(t, seriesLabels, count)
	require.True(t, sort.IsSorted(sortedByLabels(seriesLabels)))
}

type sortedByLabels []labels.Labels

func (b sortedByLabels) Len() int           { return len(b) }
func (b sortedByLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b sortedByLabels) Less(i, j int) bool { return labels.Compare(b[i], b[j]) < 0 }

func BenchmarkBatchMergeIterator(b *testing.B) {
	benchmarkChunkIteratorFunc(b, batch.NewChunkMergeIterator)
}

func BenchmarkIterMergeIterator(b *testing.B) {
	benchmarkChunkIteratorFunc(b, iterators.NewChunkMergeIterator)
}

func BenchmarkPromStorageIterator(b *testing.B) {
	benchmarkChunkIteratorFunc(b, func(chunks []chunk.Chunk, from, through model.Time) chunkenc.Iterator {
		iterables := make([]chunkenc.Iterable, len(chunks))
		for i, c := range chunks {
			iterables[i] = c.Data.SampleIterable()
		}
		return storage.ChainSampleIteratorFromIterables(nil, iterables)
	})
}

func benchmarkChunkIteratorFunc(b *testing.B, iteratorFunc chunkIteratorFunc) {
	const samplesPerChunk = 250
	type pair struct {
		t int64
		v float64
	}
	var (
		t      = int64(1234123324)
		v      = 1243535.123
		chunks []chunk.Chunk
	)
	var (
		startT int64
		endT   int64
	)
	for j := 0; j < 10; j++ {
		var exp []pair
		startT = t
		for i := 0; i < samplesPerChunk; i++ {
			// t += int64(rand.Intn(10000) + 1)
			t += int64(1000)
			// v = rand.Float64()
			v += float64(100)
			exp = append(exp, pair{t: t, v: v})
		}
		endT = t

		chk := chunkenc.NewXORChunk()
		{
			a, err := chk.Appender()
			if err != nil {
				b.Fatalf("get appender: %s", err)
			}
			k := 0
			for _, p := range exp {
				if k > 250 {
					break
				}
				a.Append(p.t, p.v)
				k++
			}
		}
		c, err := promchunk.NewForEncoding(promchunk.PrometheusXorChunk)
		require.NoError(b, err)
		chunks = append(chunks, chunk.NewChunk(nil, c, model.TimeFromUnix(util.TimeFromMillis(startT).Unix()), model.TimeFromUnix(util.TimeFromMillis(endT).Unix())))
	}

	startTT := model.TimeFromUnix(util.TimeFromMillis(startT).Unix())
	endTT := model.TimeFromUnix(util.TimeFromMillis(endT).Unix())

	b.ReportAllocs()
	b.ResetTimer()

	var res float64
	var it chunkenc.Iterator
	for i := 0; i < b.N; {
		it = iteratorFunc(chunks, startTT, endTT)
		for it.Next() == chunkenc.ValFloat {
			_, v := it.At()
			res = v
			i++
		}
		if err := it.Err(); err != nil && !errors.Is(err, io.EOF) {
			require.NoError(b, err)
		}
		_ = res
	}
}
