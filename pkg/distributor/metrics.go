package distributor

import (
	"github.com/prometheus/prometheus/model/labels"
)

type samplesPerLabelSetEntry struct {
	floatSamples     int
	histogramSamples int
	labels           labels.Labels
}
