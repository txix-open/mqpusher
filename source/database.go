package source

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/Masterminds/squirrel"
	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/db/jsonb"
	"github.com/txix-open/isp-kit/db/query"
	"github.com/txix-open/isp-kit/dbrx"
	"github.com/txix-open/isp-kit/json"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/mqpusher/conf"
	"github.com/txix-open/mqpusher/domain"
	"golang.org/x/sync/errgroup"
)

const (
	viewName      = "__mqpusher_view"
	viewIndexName = "__mqpusher_view_index"
	viewRowNum    = "__mqpusher_view_row_num"
	viewModRowNum = "__mqpusher_view_mod_row_num"
)

type dataBaseSource struct {
	db       *dbrx.Client
	logger   log.Logger
	dataChan chan *domain.Payload
	errChan  chan error

	readCounter *atomic.Uint64
	rowsCount   float64
	cfg         conf.DbDataSource
}

func NewDataBase(ctx context.Context, cfg conf.DbDataSource, logger log.Logger) (dataBaseSource, error) {
	db := dbrx.New(logger)
	cfg.Client.MaxOpenConn = 64
	err := db.Upgrade(ctx, cfg.Client)
	if err != nil {
		return dataBaseSource{}, errors.WithMessage(err, "upgrade db client")
	}

	for i, column := range cfg.SelectedColumns {
		cfg.SelectedColumns[i] = fmt.Sprintf("%s.%s", cfg.Table, column)
	}
	if len(cfg.SelectedColumns) == 0 {
		cfg.SelectedColumns = []string{fmt.Sprintf("%s.*", cfg.Table)}
	}
	cfg.WhereClause, _ = strings.CutPrefix(cfg.WhereClause, "WHERE ")

	dataSource := dataBaseSource{
		db:          db,
		logger:      logger,
		dataChan:    make(chan *domain.Payload, cfg.BatchSize*uint64(cfg.Parallel)), // nolint:gosec
		errChan:     make(chan error, 1),
		readCounter: new(atomic.Uint64),
		cfg:         cfg,
	}

	fields := append([]string{
		fmt.Sprintf("ROW_NUMBER() OVER () AS %s", viewRowNum),
		fmt.Sprintf("MOD(ROW_NUMBER() OVER (), %d) AS %s", cfg.Parallel, viewModRowNum)},
		cfg.PrimaryKey...,
	)
	q, _, err := query.New().
		Select(fields...).
		From(cfg.Table).
		Where(cfg.WhereClause).
		ToSql()
	if err != nil {
		return dataBaseSource{}, errors.WithMessage(err, "build select query")
	}
	err = dataSource.createMaterializedView(ctx, viewName, q)
	if err != nil {
		return dataBaseSource{}, errors.WithMessage(err, "create materialized view")
	}

	err = dataSource.createViewIndex(ctx)
	if err != nil {
		return dataBaseSource{}, errors.WithMessage(err, "create view index")
	}

	q = fmt.Sprintf("SELECT COUNT(1) FROM %s", viewName)
	logger.Info(ctx, "selecting rows count", log.String("query", q))
	err = db.SelectRow(ctx, &dataSource.rowsCount, q)
	if err != nil {
		return dataBaseSource{}, errors.WithMessage(err, "select view rows count")
	}
	logger.Info(ctx, fmt.Sprintf("rows count of %s: %d", viewName, int(dataSource.rowsCount)))

	go dataSource.startFetchingData(ctx)

	return dataSource, nil
}

func (d dataBaseSource) GetData(_ context.Context) (*domain.Payload, error) {
	select {
	case v, ok := <-d.dataChan:
		if !ok {
			return nil, domain.ErrNoData
		}
		d.readCounter.Add(1)
		return v, nil
	case err := <-d.errChan:
		return nil, err
	}
}

func (d dataBaseSource) startFetchingData(ctx context.Context) {
	defer close(d.dataChan)

	g, ctx := errgroup.WithContext(ctx)
	for i := range d.cfg.Parallel {
		g.Go(func() error { return d.getData(ctx, i) })
	}

	err := g.Wait()
	if err != nil {
		d.errChan <- errors.WithMessage(err, "wait")
	}
}

func (d dataBaseSource) getData(ctx context.Context, workerIdx int) error {
	cli, err := d.db.DB()
	if err != nil {
		return errors.WithMessage(err, "db cli")
	}

	columns := append([]string{viewRowNum}, d.cfg.SelectedColumns...)
	joinClause := fmt.Sprintf("%s USING (%s)", viewName, strings.Join(d.cfg.PrimaryKey, ","))
	builder := query.New().
		Select(columns...).
		From(d.cfg.Table).
		InnerJoin(joinClause)

	maxRowNum := int64(0)
	handleViewRowIdx := func(rowNum any) error {
		v, ok := (rowNum).(int64)
		if !ok {
			return errors.Errorf("cast '%s' field to int", viewRowNum)
		}
		maxRowNum = max(maxRowNum, v)
		return nil
	}
	for {
		q, args, err := builder.Where(squirrel.And{
			squirrel.Eq{viewModRowNum: workerIdx},
			squirrel.Gt{viewRowNum: maxRowNum},
		}).OrderBy(viewRowNum).
			Limit(d.cfg.BatchSize).
			ToSql()
		if err != nil {
			return errors.WithMessagef(err, "build select query to '%s' table", d.cfg.Table)
		}

		rows, err := cli.QueryContext(ctx, q, args...)
		if err != nil {
			return errors.WithMessage(err, "query context")
		}

		dataList, err := d.handleRows(ctx, rows, handleViewRowIdx)
		if err != nil {
			return errors.WithMessage(err, "handle rows")
		}
		if len(dataList) == 0 {
			return nil
		}

		for _, data := range dataList {
			d.dataChan <- &domain.Payload{Data: data}
		}
	}
}

type rowNumHandlerFunc func(rowNum any) error

const jsonbColumnType = "JSONB"

func (d dataBaseSource) handleRows(ctx context.Context, rows *sql.Rows, handleRowNum rowNumHandlerFunc) ([]map[string]any, error) {
	defer func() {
		err := rows.Close()
		if err != nil {
			d.logger.Warn(ctx, errors.WithMessage(err, "close rows"))
		}
	}()

	var (
		result  = make([]map[string]any, 0)
		columns []*sql.ColumnType
		err     error
	)
	for rows.Next() {
		if columns == nil {
			columns, err = rows.ColumnTypes()
			if err != nil {
				return nil, errors.WithMessage(err, "get column types")
			}
		}

		values := make([]any, len(columns))
		for i := range columns {
			values[i] = new(any)
		}
		err := rows.Scan(values...)
		if err != nil {
			return nil, errors.WithMessage(err, "scan row values")
		}

		data, err := d.buildColumnsMap(columns, values, handleRowNum)
		if err != nil {
			return nil, errors.WithMessage(err, "build map from columns")
		}
		result = append(result, data)
	}

	err = rows.Err()
	if err != nil {
		return nil, errors.WithMessage(err, "rows error")
	}

	return result, nil
}

func (d dataBaseSource) Progress() domain.Progress {
	readDataCount := d.readCounter.Load()
	readDataPercent := float64(readDataCount) / d.rowsCount * 100
	return domain.Progress{
		ReadDataCount:   readDataCount,
		ReadDataPercent: &readDataPercent,
	}
}

func (d dataBaseSource) Close(ctx context.Context) error {
	query := fmt.Sprintf("DROP MATERIALIZED VIEW %s CASCADE", viewName)
	d.logger.Info(ctx, "dropping materialized view", log.String("query", query))
	_, err := d.db.Exec(ctx, query)
	if err != nil {
		d.logger.Error(ctx, errors.WithMessage(err, "drop materialized view"))
	}

	err = d.db.Close()
	if err != nil {
		return errors.WithMessage(err, "close db conn")
	}

	return nil
}

func (d dataBaseSource) createMaterializedView(ctx context.Context, viewName string, query string) error {
	query = fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS %s", viewName, query)
	d.logger.Info(ctx, "creating materialized view", log.String("query", query))
	_, err := d.db.Exec(ctx, query)
	if err != nil {
		return errors.WithMessagef(err, "exec create materialized view '%s' query", viewName)
	}
	return nil
}

func (d dataBaseSource) createViewIndex(ctx context.Context) error {
	query := fmt.Sprintf("CREATE INDEX %s ON %s (%s, %s)", viewIndexName, viewName, viewRowNum, viewModRowNum)
	d.logger.Info(ctx, "creating index", log.String("query", query))
	_, err := d.db.Exec(ctx, query)
	if err != nil {
		return errors.WithMessagef(err, "exec create index '%s' query", viewIndexName)
	}
	return nil
}

func (d dataBaseSource) buildColumnsMap(columns []*sql.ColumnType, values []any, handleRowNum rowNumHandlerFunc) (map[string]any, error) {
	result := make(map[string]any, len(values))
	for i, column := range columns {
		columnName := column.Name()
		v, ok := values[i].(*any)
		if !ok {
			return nil, errors.Errorf("cast value to pointer; column = %s", columnName)
		}

		if column.DatabaseTypeName() == jsonbColumnType {
			v, err := d.handleJsonbType(*v)
			if err != nil {
				return nil, errors.WithMessagef(err, "handle jsonb type; column = %s", columnName)
			}

			result[columnName] = v
			continue
		}

		if columnName != viewRowNum {
			result[columnName] = *v
			continue
		}

		if handleRowNum == nil {
			continue
		}
		err := handleRowNum(*v)
		if err != nil {
			return nil, errors.WithMessage(err, "handle view row index")
		}
	}

	return result, nil
}

func (d dataBaseSource) handleJsonbType(v any) (any, error) {
	bytes, ok := (v).(jsonb.Type)
	if !ok {
		return nil, errors.New("cast value to jsonb pointer")
	}

	var result map[string]any
	err := json.Unmarshal(bytes, &result)
	if err != nil {
		return d.handleArray(bytes)
	}

	return result, nil
}

func (d dataBaseSource) handleArray(bytes []byte) (any, error) {
	result := make([]any, 0)
	err := json.Unmarshal(bytes, &result)
	if err != nil {
		return nil, errors.WithMessage(err, "json unmarshal")
	}

	return result, nil
}
