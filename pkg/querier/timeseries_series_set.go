package querier

import (
	"github.com/prometheus/prometheus/model/histogram"
	"sort"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

// timeSeriesSeriesSet is a wrapper around a cortexpb.TimeSeries slice to implement to SeriesSet interface
type timeSeriesSeriesSet struct {
	ts []cortexpb.TimeSeries
	i  int
}

func newTimeSeriesSeriesSet(sortSeries bool, series []cortexpb.TimeSeries) *timeSeriesSeriesSet {
	if sortSeries {
		sort.Sort(byTimeSeriesLabels(series))
	}

	return &timeSeriesSeriesSet{
		ts: series,
		i:  -1,
	}
}

// Next implements storage.SeriesSet interface.
func (t *timeSeriesSeriesSet) Next() bool { t.i++; return t.i < len(t.ts) }

// At implements storage.SeriesSet interface.
func (t *timeSeriesSeriesSet) At() storage.Series {
	if t.i < 0 {
		return nil
	}
	return &timeseries{series: t.ts[t.i]}
}

// Err implements storage.SeriesSet interface.
func (t *timeSeriesSeriesSet) Err() error { return nil }

// Warnings implements storage.SeriesSet interface.
func (t *timeSeriesSeriesSet) Warnings() annotations.Annotations { return nil }

// timeseries is a type wrapper that implements the storage.Series interface
type timeseries struct {
	series cortexpb.TimeSeries
}

// timeSeriesSeriesIterator is a wrapper around a cortexpb.TimeSeries to implement the SeriesIterator interface
type timeSeriesSeriesIterator struct {
	curType      chunkenc.ValueType
	ts           *timeseries
	cur          int
	curHistogram int
}

type byTimeSeriesLabels []cortexpb.TimeSeries

func (b byTimeSeriesLabels) Len() int      { return len(b) }
func (b byTimeSeriesLabels) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byTimeSeriesLabels) Less(i, j int) bool {
	return labels.Compare(cortexpb.FromLabelAdaptersToLabels(b[i].Labels), cortexpb.FromLabelAdaptersToLabels(b[j].Labels)) < 0
}

// Labels implements the storage.Series interface.
// Conversion is safe because ingester sets these by calling client.FromLabelsToLabelAdapters which guarantees labels are sorted.
func (t *timeseries) Labels() labels.Labels {
	return cortexpb.FromLabelAdaptersToLabels(t.series.Labels)
}

// Iterator implements the storage.Series interface
func (t *timeseries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return &timeSeriesSeriesIterator{
		ts:           t,
		cur:          -1,
		curHistogram: -1,
		curType:      chunkenc.ValNone,
	}
}

// Seek implements SeriesIterator interface
func (t *timeSeriesSeriesIterator) Seek(s int64) chunkenc.ValueType {
	offset := 0
	if t.cur > 0 {
		offset = t.cur // only advance via Seek
	}

	t.cur = sort.Search(len(t.ts.series.Samples[offset:]), func(i int) bool {
		return t.ts.series.Samples[offset+i].TimestampMs >= s
	}) + offset

	t.curHistogram = sort.Search(len(t.ts.series.Histograms[offset:]), func(i int) bool {
		return t.ts.series.Histograms[offset+i].TimestampMs >= s
	}) + offset

	if t.cur >= len(t.ts.series.Samples) && t.curHistogram >= len(t.ts.series.Histograms) {
		return chunkenc.ValNone
	}

	if t.cur >= len(t.ts.series.Samples) || t.ts.series.Histograms[t.curHistogram].TimestampMs <= t.ts.series.Samples[t.cur].TimestampMs {
		if t.ts.series.Histograms[t.curHistogram].IsFloatHistogram() {
			t.curType = chunkenc.ValFloatHistogram
		} else {
			t.curType = chunkenc.ValHistogram
		}
		return t.curType
	}
	t.curType = chunkenc.ValFloat
	return t.curType
}

// At implements the SeriesIterator interface
func (t *timeSeriesSeriesIterator) At() (int64, float64) {
	if t.curType != chunkenc.ValFloat {
		panic("cannot call At on non float chunks")
	}
	if t.cur < 0 || t.cur >= len(t.ts.series.Samples) {
		return 0, 0
	}
	return t.ts.series.Samples[t.cur].TimestampMs, t.ts.series.Samples[t.cur].Value
}

func (t *timeSeriesSeriesIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	if t.curType != chunkenc.ValHistogram {
		panic("cannot call At on non histogram chunks")
	}
	if t.cur < 0 || t.cur >= len(t.ts.series.Histograms) {
		return 0, nil
	}

	return t.ts.series.Samples[t.cur].TimestampMs, cortexpb.HistogramProtoToHistogram(t.ts.series.Histograms[t.cur])
}

func (t *timeSeriesSeriesIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	if t.curType != chunkenc.ValFloatHistogram {
		panic("cannot call At on non float histogram chunks")
	}
	if t.cur < 0 || t.cur >= len(t.ts.series.Histograms) {
		return 0, nil
	}
	return t.ts.series.Samples[t.cur].TimestampMs, cortexpb.FloatHistogramProtoToFloatHistogram(t.ts.series.Histograms[t.cur])
}

func (t *timeSeriesSeriesIterator) AtT() int64 {
	if t.curType == chunkenc.ValNone {
		return 0
	}
	if t.curType == chunkenc.ValHistogram || t.curType == chunkenc.ValFloatHistogram {
		return t.ts.series.Histograms[t.curHistogram].TimestampMs
	}
	return t.ts.series.Samples[t.cur].TimestampMs
}

// Next implements the SeriesIterator interface
func (t *timeSeriesSeriesIterator) Next() chunkenc.ValueType {
	if t.cur+1 >= len(t.ts.series.Samples) && t.curHistogram+1 >= len(t.ts.series.Histograms) {
		t.cur = len(t.ts.series.Samples)
		t.curHistogram = len(t.ts.series.Histograms)
		return chunkenc.ValNone
	}
	if t.cur+1 >= len(t.ts.series.Samples) {
		t.curHistogram++
		h := t.ts.series.Histograms[t.curHistogram]
		if h.IsFloatHistogram() {
			t.curType = chunkenc.ValFloatHistogram
		} else {
			t.curType = chunkenc.ValHistogram
		}
		return t.curType
	}
	if t.curHistogram+1 >= len(t.ts.series.Histograms) {
		t.cur++
		t.curType = chunkenc.ValFloat
		return t.curType
	}

	if t.ts.series.Histograms[t.curHistogram+1].TimestampMs <= t.ts.series.Samples[t.cur+1].TimestampMs {
		t.curHistogram++
		if t.ts.series.Histograms[t.curHistogram].IsFloatHistogram() {
			t.curType = chunkenc.ValFloatHistogram
		} else {
			t.curType = chunkenc.ValHistogram
		}
		return t.curType
	}

	t.cur++
	t.curType = chunkenc.ValFloat
	return t.curType
}

// Err implements the SeriesIterator interface
func (t *timeSeriesSeriesIterator) Err() error { return nil }
