package querier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
)

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
						var queryEngine v1.QueryEngine
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
