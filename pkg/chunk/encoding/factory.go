package encoding

import (
	"fmt"
)

// Encoding defines which encoding we are using.
type Encoding byte

var (
	// DefaultEncoding exported for use in unit tests elsewhere
	DefaultEncoding = PrometheusXorChunk
)

// String implements flag.Value.
func (e Encoding) String() string {
	if known, found := encodings[e]; found {
		return known.Name
	}
	return fmt.Sprintf("%d", e)
}

const (
	// PrometheusXorChunk is a wrapper around Prometheus XOR-encoded chunk.
	// 4 is the magic value for backwards-compatibility with previous iota-based constants.
	PrometheusXorChunk Encoding = 4
	// PrometheusHistogramChunk is a wrapper around Prometheus histogram chunk.
	// 5 is the magic value for backwards-compatibility with previous iota-based constants.
	PrometheusHistogramChunk Encoding = 5
	// PrometheusFloatHistogramChunk is a wrapper around Prometheus float histogram chunk.
	// 6 is the magic value for backwards-compatibility with previous iota-based constants.
	PrometheusFloatHistogramChunk Encoding = 6
)

type encoding struct {
	Name string
	New  func() Chunk
}

var encodings = map[Encoding]encoding{
	PrometheusXorChunk: {
		Name: "PrometheusXorChunk",
		New: func() Chunk {
			return newPrometheusXorChunk()
		},
	},
	PrometheusHistogramChunk: {
		Name: "PrometheusHistogramChunk",
		New: func() Chunk {
			return newPrometheusHistogramChunk()
		},
	},
	PrometheusFloatHistogramChunk: {
		Name: "PrometheusFloatHistogramChunk",
		New: func() Chunk {
			return newPrometheusFloatHistogramChunk()
		},
	},
}

// NewForEncoding allows configuring what chunk type you want
func NewForEncoding(encoding Encoding) (Chunk, error) {
	enc, ok := encodings[encoding]
	if !ok {
		return nil, fmt.Errorf("unknown chunk encoding: %v", encoding)
	}

	return enc.New(), nil
}
