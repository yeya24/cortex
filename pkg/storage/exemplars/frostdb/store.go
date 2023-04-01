package frostdb

import (
	"context"
	"fmt"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/go-kit/log"
	"github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"
	"go.opentelemetry.io/otel/trace"
)

const (
	tableName = "exemplars"
)

type FrostDBStore struct {
	table  *frostdb.Table
	schema *dynparquet.Schema
	engine *query.LocalEngine
}

func NewFrostDBStore(cfg Config, logger log.Logger, tracer trace.Tracer, reg prometheus.Registerer, dbName string, ingester bool) (*FrostDBStore, error) {
	ctx := context.TODO()
	bucketClient, err := bucket.NewClient(ctx, cfg.Config, "exemplars-storage", logger, reg)
	if err != nil {
		return nil, err
	}
	opts := []frostdb.Option{
		frostdb.WithLogger(logger),
		frostdb.WithRegistry(reg),
		frostdb.WithTracer(tracer),
		frostdb.WithBucketStorage(bucketClient),
		frostdb.WithActiveMemorySize(cfg.ActiveMemorySize),
	}
	// If it is ingester store, we want to enable WAL and read local data only.
	// For long term storage, we don't need WAL because it is read only.
	if ingester {
		opts = append(opts, frostdb.WithIgnoreStorageOnQuery(), frostdb.WithWAL(), frostdb.WithStoragePath(cfg.Path))
	}
	store, err := frostdb.New(opts...)
	if err != nil {
		return nil, err
	}
	db, err := store.DB(ctx, dbName)
	if err != nil {
		return nil, err
	}

	schema, err := exemplarSchema()
	if err != nil {
		return nil, err
	}

	engine := query.NewEngine(memory.DefaultAllocator, db.TableProvider())
	table, err := db.Table(
		tableName,
		frostdb.NewTableConfig(schema.Definition()),
	)
	if err != nil {
		return nil, err
	}
	return &FrostDBStore{
		table:  table,
		schema: schema,
		engine: engine,
	}, nil
}

func (s *FrostDBStore) AppendExemplar(ctx context.Context, tenant string, lset labels.Labels, e exemplar.Exemplar) error {
	dynamicColumnLabels := make([]string, 0, len(lset))
	dynamicColumnExemplarLabels := make([]string, 0, len(e.Labels))
	row := make([]parquet.Value, 0, len(lset)+len(e.Labels))
	for _, lbl := range lset {
		dynamicColumnLabels = append(dynamicColumnLabels, lbl.Name)
	}
	for _, lbl := range e.Labels {
		dynamicColumnExemplarLabels = append(dynamicColumnExemplarLabels, lbl.Name)
	}

	buf, err := s.schema.NewBuffer(map[string][]string{
		ColumnLabels:         dynamicColumnLabels,
		ColumnExemplarLabels: dynamicColumnExemplarLabels,
	})
	if err != nil {
		return err
	}

	// schema.Columns() returns a sorted list of all columns.
	// We match on the column's name to insert the correct values.
	// We track the columnIndex to insert each column at the correct index.
	columnIndex := 0
	for _, column := range s.schema.Columns() {
		switch column.Name {
		case ColumnTenant:
			row = append(row, parquet.ValueOf(tenant).Level(0, 0, columnIndex))
			columnIndex++
		case ColumnLabels:
			for _, v := range lset {
				row = append(row, parquet.ValueOf(v.Value).Level(0, 1, columnIndex))
				columnIndex++
			}
		case ColumnExemplarLabels:
			for _, v := range e.Labels {
				row = append(row, parquet.ValueOf(v.Value).Level(0, 1, columnIndex))
				columnIndex++
			}
		case ColumnTimestamp:
			row = append(row, parquet.ValueOf(e.Ts).Level(0, 0, columnIndex))
			columnIndex++
		case ColumnValue:
			row = append(row, parquet.ValueOf(e.Value).Level(0, 0, columnIndex))
			columnIndex++
		default:
		}
	}

	if _, err = buf.WriteRows([]parquet.Row{row}); err != nil {
		return err
	}

	if _, err = s.table.InsertBuffer(ctx, buf); err != nil {
		return err
	}

	return nil
}

func (s *FrostDBStore) Select(ctx context.Context, tenant string, start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	seriesSet := map[uint64]*exemplar.QueryResult{}
	for _, matcher := range matchers {
		s.engine.ScanTable(tableName).
			Filter(
				logicalplan.And(
					logicalplan.Col(ColumnTenant).Eq(logicalplan.Literal(tenant)),
					logicalplan.And(
						logicalplan.Col(ColumnTimestamp).GtEq(logicalplan.Literal(start)),
						logicalplan.Col(ColumnTimestamp).LtEq(logicalplan.Literal(end)),
					),
					promMatchersToFrostDBExprs(matcher),
				),
			).
			Project(
				logicalplan.DynCol(ColumnLabels),
				logicalplan.DynCol(ColumnExemplarLabels),
				logicalplan.Col(ColumnTimestamp),
				logicalplan.Col(ColumnValue),
			).
			Execute(ctx, func(ctx context.Context, r arrow.Record) error {
				var (
					ts int64
					v  float64
				)
				for i := 0; i < int(r.NumRows()); i++ {
					lbls := labels.Labels{}
					exemplarLabels := labels.Labels{}
					for j := 0; j < int(r.NumCols()); j++ {
						switch {
						case r.ColumnName(j) == ColumnTenant:
							continue
						case r.ColumnName(j) == ColumnTimestamp:
							ts = r.Column(j).(*array.Int64).Value(i)
						case r.ColumnName(j) == ColumnValue:
							v = r.Column(j).(*array.Float64).Value(i)
						case strings.HasPrefix(r.ColumnName(j), "labels."):
							name := strings.TrimPrefix(r.ColumnName(j), "labels.")
							dict, ok := r.Column(j).(*array.Dictionary)
							if !ok {
								return fmt.Errorf("expected dictionary column, got %T", r.Column(j))
							}

							if dict.IsNull(i) {
								continue
							}

							val := StringValueFromDictionary(dict, i)

							// Because of an implementation detail of aggregations in
							// FrostDB resulting columns can have the value of "", but that
							// is equivalent to the label not existing at all, so we need
							// to skip it.
							if len(val) > 0 {
								lbls = append(lbls, labels.Label{Name: name, Value: val})
							}
						default:
							name := strings.TrimPrefix(r.ColumnName(j), "exemplar_labels.")
							dict, ok := r.Column(j).(*array.Dictionary)
							if !ok {
								return fmt.Errorf("expected dictionary column, got %T", r.Column(j))
							}

							if dict.IsNull(i) {
								continue
							}

							val := StringValueFromDictionary(dict, i)

							// Because of an implementation detail of aggregations in
							// FrostDB resulting columns can have the value of "", but that
							// is equivalent to the label not existing at all, so we need
							// to skip it.
							if len(val) > 0 {
								exemplarLabels = append(exemplarLabels, labels.Label{Name: name, Value: val})
							}
						}
					}
					h := lbls.Hash()
					if es, ok := seriesSet[h]; ok {
						es.Exemplars = append(es.Exemplars, exemplar.Exemplar{
							Labels: exemplarLabels,
							Ts:     ts,
							Value:  v,
						})
					} else {
						seriesSet[h] = &exemplar.QueryResult{
							SeriesLabels: lbls,
							Exemplars: []exemplar.Exemplar{
								{
									Labels: exemplarLabels,
									Ts:     ts,
									Value:  v,
								},
							},
						}
					}
				}
				return nil
			})
	}

	res := make([]exemplar.QueryResult, 0, len(seriesSet))
	for _, v := range seriesSet {
		res = append(res, *v)
	}
	return res, nil
}

func promMatchersToFrostDBExprs(matchers []*labels.Matcher) logicalplan.Expr {
	exprs := []logicalplan.Expr{}
	for _, matcher := range matchers {
		switch matcher.Type {
		case labels.MatchEqual:
			exprs = append(exprs, logicalplan.Col("labels."+matcher.Name).Eq(logicalplan.Literal(matcher.Value)))
		case labels.MatchNotEqual:
			exprs = append(exprs, logicalplan.Col("labels."+matcher.Name).NotEq(logicalplan.Literal(matcher.Value)))
		case labels.MatchRegexp:
			exprs = append(exprs, logicalplan.Col("labels."+matcher.Name).RegexMatch(matcher.Value))
		case labels.MatchNotRegexp:
			exprs = append(exprs, logicalplan.Col("labels."+matcher.Name).RegexNotMatch(matcher.Value))
		}
	}
	return logicalplan.And(exprs...)
}

func StringValueFromDictionary(arr *array.Dictionary, i int) string {
	switch dict := arr.Dictionary().(type) {
	case *array.Binary:
		return string(dict.Value(arr.GetValueIndex(i)))
	case *array.String:
		return dict.Value(arr.GetValueIndex(i))
	default:
		panic(fmt.Sprintf("unsupported dictionary type: %T", dict))
	}
}
