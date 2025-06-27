package iceberg

import (
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

func NewSeriesSet(series []storage.Series) storage.SeriesSet {
	return &concreteSeries{
		series: series,
		curr:   -1,
	}
}

type concreteSeries struct {
	series []storage.Series
	curr   int
}

func (c *concreteSeries) Next() bool {
	c.curr++
	return c.curr < len(c.series)
}

func (c *concreteSeries) At() storage.Series {
	return c.series[c.curr]
}

func (c *concreteSeries) Err() error {
	return nil
}

func (c *concreteSeries) Warnings() annotations.Annotations {
	return nil
}
