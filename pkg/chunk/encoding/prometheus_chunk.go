package encoding

import (
	"bytes"
	"io"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Wrapper around Prometheus chunk.
type prometheusXorChunk struct {
	chunk chunkenc.Chunk
}

func newPrometheusXorChunk() *prometheusXorChunk {
	return &prometheusXorChunk{}
}

// Add adds another sample to the chunk. While Add works, it is only implemented
// to make tests work, and should not be used in production. In particular, it appends
// all samples to single chunk, and uses new Appender for each Add.
func (p *prometheusXorChunk) Add(m model.SamplePair) (Chunk, error) {
	if p.chunk == nil {
		p.chunk = chunkenc.NewXORChunk()
	}

	app, err := p.chunk.Appender()
	if err != nil {
		return nil, err
	}

	app.Append(int64(m.Timestamp), float64(m.Value))
	return nil, nil
}

func (p *prometheusXorChunk) AddHistogram(int64, *histogram.Histogram) (Chunk, error) {
	return nil, errors.New("appended a histogram sample to a float chunk")
}

func (p *prometheusXorChunk) AddFloatHistogram(int64, *histogram.FloatHistogram) (Chunk, error) {
	return nil, errors.New("appended a float histogram sample to a float chunk")
}

func (p *prometheusXorChunk) NewIterator(iterator Iterator) Iterator {
	if p.chunk == nil {
		return errorIterator("Prometheus chunk is not set")
	}

	if pit, ok := iterator.(*prometheusChunkIterator); ok {
		pit.c = p.chunk
		pit.it = p.chunk.Iterator(pit.it)
		return pit
	}

	return &prometheusChunkIterator{c: p.chunk, it: p.chunk.Iterator(nil)}
}

func (p *prometheusXorChunk) Marshal(i io.Writer) error {
	if p.chunk == nil {
		return errors.New("chunk data not set")
	}
	_, err := i.Write(p.chunk.Bytes())
	return err
}

func (p *prometheusXorChunk) UnmarshalFromBuf(bytes []byte) error {
	c, err := chunkenc.FromData(chunkenc.EncXOR, bytes)
	if err != nil {
		return errors.Wrap(err, "failed to create Prometheus chunk from bytes")
	}

	p.chunk = c
	return nil
}

func (p *prometheusXorChunk) Encoding() Encoding {
	return PrometheusXorChunk
}

func (p *prometheusXorChunk) Len() int {
	if p.chunk == nil {
		return 0
	}
	return p.chunk.NumSamples()
}

// Wrapper around Prometheus histogram chunk.
type prometheusHistogramChunk struct {
	chunk chunkenc.Chunk
}

func newPrometheusHistogramChunk() *prometheusHistogramChunk {
	return &prometheusHistogramChunk{}
}

// Add adds another sample to the chunk. While Add works, it is only implemented
// to make tests work, and should not be used in production. In particular, it appends
// all samples to single chunk, and uses new Appender for each Add.
func (p *prometheusHistogramChunk) Add(m model.SamplePair) (Chunk, error) {
	return nil, errors.New("appended a float sample to a histogram chunk")
}

func (p *prometheusHistogramChunk) AddHistogram(t int64, h *histogram.Histogram) (Chunk, error) {
	if p.chunk == nil {
		p.chunk = chunkenc.NewHistogramChunk()
	}

	app, err := p.chunk.Appender()
	if err != nil {
		return nil, err
	}

	_, _, _, err = app.AppendHistogram(nil, t, h, true)
	return nil, err
}

func (p *prometheusHistogramChunk) AddFloatHistogram(int64, *histogram.FloatHistogram) (Chunk, error) {
	return nil, errors.New("appended a float histogram sample to a histogram chunk")
}

func (p *prometheusHistogramChunk) NewIterator(iterator Iterator) Iterator {
	if p.chunk == nil {
		return errorIterator("Prometheus chunk is not set")
	}

	if pit, ok := iterator.(*prometheusChunkIterator); ok {
		pit.c = p.chunk
		pit.it = p.chunk.Iterator(pit.it)
		return pit
	}

	return &prometheusChunkIterator{c: p.chunk, it: p.chunk.Iterator(nil)}
}

func (p *prometheusHistogramChunk) Marshal(i io.Writer) error {
	if p.chunk == nil {
		return errors.New("chunk data not set")
	}
	_, err := i.Write(p.chunk.Bytes())
	return err
}

func (p *prometheusHistogramChunk) UnmarshalFromBuf(bytes []byte) error {
	c, err := chunkenc.FromData(chunkenc.EncHistogram, bytes)
	if err != nil {
		return errors.Wrap(err, "failed to create Prometheus chunk from bytes")
	}

	p.chunk = c
	return nil
}

func (p *prometheusHistogramChunk) Encoding() Encoding {
	return PrometheusHistogramChunk
}

func (p *prometheusHistogramChunk) Len() int {
	if p.chunk == nil {
		return 0
	}
	return p.chunk.NumSamples()
}

// Wrapper around Prometheus float histogram chunk.
type prometheusFloatHistogramChunk struct {
	chunk chunkenc.Chunk
}

func newPrometheusFloatHistogramChunk() *prometheusFloatHistogramChunk {
	return &prometheusFloatHistogramChunk{}
}

// Add adds another sample to the chunk. While Add works, it is only implemented
// to make tests work, and should not be used in production. In particular, it appends
// all samples to single chunk, and uses new Appender for each Add.
func (p *prometheusFloatHistogramChunk) Add(m model.SamplePair) (Chunk, error) {
	return nil, errors.New("appended a float sample to a float histogram chunk")
}

func (p *prometheusFloatHistogramChunk) AddHistogram(int64, *histogram.Histogram) (Chunk, error) {
	return nil, errors.New("appended a histogram sample to a float histogram chunk")
}

func (p *prometheusFloatHistogramChunk) AddFloatHistogram(t int64, fh *histogram.FloatHistogram) (Chunk, error) {
	if p.chunk == nil {
		p.chunk = chunkenc.NewFloatHistogramChunk()
	}

	app, err := p.chunk.Appender()
	if err != nil {
		return nil, err
	}

	_, _, _, err = app.AppendFloatHistogram(nil, t, fh, true)
	return nil, err
}

func (p *prometheusFloatHistogramChunk) NewIterator(iterator Iterator) Iterator {
	if p.chunk == nil {
		return errorIterator("Prometheus chunk is not set")
	}

	if pit, ok := iterator.(*prometheusChunkIterator); ok {
		pit.c = p.chunk
		pit.it = p.chunk.Iterator(pit.it)
		return pit
	}

	return &prometheusChunkIterator{c: p.chunk, it: p.chunk.Iterator(nil)}
}

func (p *prometheusFloatHistogramChunk) Marshal(i io.Writer) error {
	if p.chunk == nil {
		return errors.New("chunk data not set")
	}
	_, err := i.Write(p.chunk.Bytes())
	return err
}

func (p *prometheusFloatHistogramChunk) UnmarshalFromBuf(bytes []byte) error {
	c, err := chunkenc.FromData(chunkenc.EncFloatHistogram, bytes)
	if err != nil {
		return errors.Wrap(err, "failed to create Prometheus chunk from bytes")
	}

	p.chunk = c
	return nil
}

func (p *prometheusFloatHistogramChunk) Encoding() Encoding {
	return PrometheusFloatHistogramChunk
}

func (p *prometheusFloatHistogramChunk) Len() int {
	if p.chunk == nil {
		return 0
	}
	return p.chunk.NumSamples()
}

type prometheusChunkIterator struct {
	c  chunkenc.Chunk // we need chunk, because FindAtOrAfter needs to start with fresh iterator.
	it chunkenc.Iterator
}

func (p *prometheusChunkIterator) Scan() chunkenc.ValueType {
	return p.it.Next()
}

func (p *prometheusChunkIterator) FindAtOrAfter(time model.Time) chunkenc.ValueType {
	// FindAtOrAfter must return OLDEST value at given time. That means we need to start with a fresh iterator,
	// otherwise we cannot guarantee OLDEST.
	p.it = p.c.Iterator(p.it)
	return p.it.Seek(int64(time))
}

func (p *prometheusChunkIterator) Value() model.SamplePair {
	ts, val := p.it.At()
	return model.SamplePair{
		Timestamp: model.Time(ts),
		Value:     model.SampleValue(val),
	}
}

func (p *prometheusChunkIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	return p.it.AtHistogram(h)
}

func (p *prometheusChunkIterator) AtFloatHistogram(h *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return p.it.AtFloatHistogram(h)
}

func (p *prometheusChunkIterator) Batch(size int, valType chunkenc.ValueType) Batch {
	var batch Batch
	j := 0
	for j < size {
		switch valType {
		case chunkenc.ValNone:
			break
		case chunkenc.ValFloat:
			t, v := p.it.At()
			batch.Timestamps[j] = t
			batch.Values[j] = v
		case chunkenc.ValHistogram:
			t, v := p.it.AtHistogram(nil)
			batch.Timestamps[j] = t
			batch.Histograms[j] = v
		case chunkenc.ValFloatHistogram:
			t, v := p.it.AtFloatHistogram(nil)
			batch.Timestamps[j] = t
			batch.FloatHistograms[j] = v
		}

		j++
		if j < size && p.it.Next() == chunkenc.ValNone {
			break
		}
	}
	batch.Index = 0
	batch.Length = j
	return batch
}

func (p *prometheusChunkIterator) Err() error {
	return p.it.Err()
}

func (p *prometheusXorChunk) Equals(chunk Chunk) (bool, error) {
	po, ok := chunk.(*prometheusXorChunk)
	if !ok {
		return false, errors.New("other chunk is not a prometheusXorChunk")
	}
	return bytes.Equal(p.chunk.Bytes(), po.chunk.Bytes()), nil
}

func (p *prometheusHistogramChunk) Equals(chunk Chunk) (bool, error) {
	po, ok := chunk.(*prometheusHistogramChunk)
	if !ok {
		return false, errors.New("other chunk is not a prometheusHistogramChunk")
	}
	return bytes.Equal(p.chunk.Bytes(), po.chunk.Bytes()), nil
}

func (p *prometheusFloatHistogramChunk) Equals(chunk Chunk) (bool, error) {
	po, ok := chunk.(*prometheusFloatHistogramChunk)
	if !ok {
		return false, errors.New("other chunk is not a prometheusFloatHistogramChunk")
	}
	return bytes.Equal(p.chunk.Bytes(), po.chunk.Bytes()), nil
}

type errorIterator string

func (e errorIterator) Scan() chunkenc.ValueType                         { return chunkenc.ValNone }
func (e errorIterator) FindAtOrAfter(time model.Time) chunkenc.ValueType { return chunkenc.ValNone }
func (e errorIterator) Value() model.SamplePair                          { panic("no values") }
func (e errorIterator) AtHistogram(_ *histogram.Histogram) (int64, *histogram.Histogram) {
	panic("no values")
}
func (e errorIterator) AtFloatHistogram(_ *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	panic("no values")
}
func (e errorIterator) Batch(size int, valType chunkenc.ValueType) Batch { panic("no values") }
func (e errorIterator) Err() error                                       { return errors.New(string(e)) }
