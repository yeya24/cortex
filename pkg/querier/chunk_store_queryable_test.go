package querier

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
)

// Make sure that chunkSeries implements SeriesWithChunks
var _ SeriesWithChunks = &chunkSeries{}

func TestChunkQueryable(t *testing.T) {
	for _, testcase := range testcases {
		for _, encoding := range encodings {
			for _, query := range queries {
				t.Run(fmt.Sprintf("%s/%s/%s", testcase.name, encoding.name, query.query), func(t *testing.T) {
					store, from := makeMockChunkStore(t, 24, encoding.e)
					queryable := newMockStoreQueryable(store, testcase.f)
					testRangeQuery(t, queryable, from, query)
				})
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

func TestCountByteSize(t *testing.T) {
	seriesNum := 12_000_000
	labelCount := 19
	labelLength := 20
	labelStr := make([]string, 0, labelCount*2)
	//series := make([]*storepb.Series, seriesNum)
	lblKeyPrefix := "label_key_" + strings.Repeat("k", labelLength)
	lblValPrefix := "label_val_" + strings.Repeat("v", labelLength)
	size := 0

	vector := make([]promql.Sample, 0, seriesNum)
	for i := 0; i < seriesNum; i++ {
		c := chunkenc.NewXORChunk()
		a, err := c.Appender()
		require.NoError(t, err)
		t := time.Now().UnixMilli()
		v := float64(i)
		a.Append(t, v)
		labelStr = labelStr[:0]
		for j := 0; j < labelCount; j++ {
			labelStr = append(labelStr, fmt.Sprintf("%s_%d_%d", lblKeyPrefix, i, j))
			labelStr = append(labelStr, fmt.Sprintf("%s_%d_%d", lblValPrefix, i, j))
		}
		lbls := mkZLabels(labelStr...)
		x := &storepb.Series{
			Labels: lbls,
			Chunks: []storepb.AggrChunk{{Raw: &storepb.Chunk{
				Type: storepb.Chunk_XOR,
				Data: c.Bytes(),
			}}},
		}
		size += x.Size()
		vector = append(vector, promql.Sample{
			Metric: labelpb.ZLabelsToPromLabels(lbls),
			Point: promql.Point{
				T: t,
				V: v,
			},
		})
	}

	fmt.Println(float64(size) / 1000 / 1000 / 1000)

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status: statusSuccess,
		Data: queryData{
			ResultType: "vector",
			Result:     promql.Vector(vector),
		},
	})
	require.NoError(t, err)

	buf := bytes.NewBuffer(b)
	gReader, err := gzip.NewReader(buf)
	require.NoError(t, err)
	gzipped, err := io.ReadAll(gReader)
	require.NoError(t, err)
	fmt.Printf("Size in storepb: %f GB, before gzipped response: %f GB, after gzipped response: %f GB\n", float64(size)/1000/1000/1000, float64(len(b))/1000/1000/1000, float64(len(gzipped))/1000/1000/1000)
}

type errorType string

const (
	errorNone        errorType = ""
	errorTimeout     errorType = "timeout"
	errorCanceled    errorType = "canceled"
	errorExec        errorType = "execution"
	errorBadData     errorType = "bad_data"
	errorInternal    errorType = "internal"
	errorUnavailable errorType = "unavailable"
	errorNotFound    errorType = "not_found"
)

type response struct {
	Status    string      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

type queryData struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
	Stats      stats.QueryStats `json:"stats,omitempty"`
}
