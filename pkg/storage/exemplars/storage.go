package exemplars

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/trace"

	"github.com/cortexproject/cortex/pkg/storage/exemplars/frostdb"
)

type ExemplarStoreType string

const (
	FrostDBExemplarStore ExemplarStoreType = "frostdb"
)

type ExemplarStore interface {
	ExemplarAppender
	ExemplarQuerier
}

type ExemplarAppender interface {
	AppendExemplar(ctx context.Context, tenant string, lset labels.Labels, e exemplar.Exemplar) error
}

type ExemplarQuerier interface {
	Select(ctx context.Context, tenant string, start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error)
}

type ExemplarStoreConfig struct {
	Backend string         `yaml:"backend"`
	FrostDB frostdb.Config `yaml:"frostdb"`
}

func NewExemplarStore(
	cfg ExemplarStoreConfig,
	logger log.Logger,
	tracer trace.Tracer,
	reg prometheus.Registerer,
	ingester bool,
) (ExemplarStore, error) {
	switch cfg.Backend {
	case string(FrostDBExemplarStore):
		return frostdb.NewFrostDBStore(cfg.FrostDB, logger, tracer, reg, "exemplars", ingester)
	}
	return nil, nil
}
