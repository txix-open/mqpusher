package source

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"

	"github.com/integration-system/isp-lib/v2/atomic"
	"github.com/integration-system/mqpusher/conf"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

const (
	defaultIdColumn = "id"
	defaultSelect   = "*"
)

type ConcurrentDbDataSource struct {
	cfg           conf.DBSource
	conCfg        conf.ConcurrentDBSource
	errCh         chan error
	rowsCh        chan map[string]interface{}
	db            *pgxpool.Pool
	totalRows     int64
	processedRows *atomic.AtomicInt
}

func (c *ConcurrentDbDataSource) GetData() (interface{}, error) {
	select {
	case err := <-c.errCh:
		return nil, err
	case row, open := <-c.rowsCh:
		if !open {
			return nil, nil
		}
		c.processedRows.IncAndGet()
		return row, nil
	}
}

func (c *ConcurrentDbDataSource) Progress() (int64, float32) {
	current := int64(c.processedRows.Get())
	return current, float32(current) / float32(c.totalRows) * 100
}

func (c *ConcurrentDbDataSource) Close() error {
	c.db.Close()
	return nil
}

func (c *ConcurrentDbDataSource) startFetching() {
	cfg := c.conCfg
	aggQuery := fmt.Sprintf("SELECT min(%s), max(%s) FROM %s", cfg.IdColumn, cfg.IdColumn, cfg.Table)
	row := c.db.QueryRow(context.Background(), aggQuery)
	min := 0
	max := 0
	if err := row.Scan(&min, &max); err != nil {
		c.notifyErr(errors.WithMessage(err, "get (min, max) id"))
		return
	}
	if max == 0 {
		close(c.rowsCh)
		return
	}
	offsetId := min / batchSize
	queriesCount := int(math.Ceil(float64(max-min) / float64(batchSize)))
	if queriesCount == 0 {
		queriesCount = 1
	}
	currentQuery := atomic.NewAtomicInt(0)
	fetching := atomic.NewAtomicBool(true)
	goroutinesCount := runtime.NumCPU() * runtime.NumCPU()
	wg := sync.WaitGroup{}
	errChan := make(chan error, goroutinesCount)
	done := make(chan struct{})

	q := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s > $1 AND %s <= $2 %s ORDER BY %s LIMIT $3",
		cfg.Select, cfg.Table, cfg.IdColumn, cfg.IdColumn, cfg.Where, cfg.IdColumn,
	)
	for i := 0; i < goroutinesCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var columns []string
			for fetching.Get() {
				qNum := currentQuery.IncAndGet()
				if qNum > queriesCount {
					fetching.Set(false)
					return
				}

				currentId := (qNum + offsetId - 1) * batchSize
				rows, err := c.db.Query(context.Background(), q, currentId, currentId+batchSize, batchSize)
				if err != nil {
					errChan <- errors.WithMessage(err, "select data")
					return
				}
				for rows.Next() {
					if columns == nil {
						fieldDescriptions := rows.FieldDescriptions()
						columns = make([]string, len(fieldDescriptions))
						for i := range fieldDescriptions {
							columns[i] = string(fieldDescriptions[i].Name)
						}
					}
					values, err := rows.Values()
					if err != nil {
						errChan <- errors.WithMessage(err, "extract result values")
						return
					}
					row := make(map[string]interface{}, len(values))
					for i := range columns {
						row[columns[i]] = values[i]
					}
					c.rowsCh <- row
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case err := <-errChan:
		fetching.Set(false)
		wg.Wait()
		c.notifyErr(err)
	case <-done:
		close(c.rowsCh)
	}
}

func (c *ConcurrentDbDataSource) notifyErr(err error) {
	c.errCh <- err
}

func NewConcurrentDbDataSource(cfg conf.DBSource, concurrentCfg conf.ConcurrentDBSource) (DataSource, error) {
	db, err := pgxpool.Connect(context.Background(), sqlConnString(cfg.Database))
	if err != nil {
		return nil, err
	}

	totalRows, err := EstimateQueryTotalRows(db, cfg.Query)
	if err != nil {
		return nil, err
	}

	if concurrentCfg.IdColumn == "" {
		concurrentCfg.IdColumn = defaultIdColumn
	}
	if concurrentCfg.Select == "" {
		concurrentCfg.Select = defaultSelect
	}
	ds := &ConcurrentDbDataSource{
		cfg:           cfg,
		conCfg:        concurrentCfg,
		db:            db,
		totalRows:     totalRows,
		rowsCh:        make(chan map[string]interface{}),
		errCh:         make(chan error),
		processedRows: atomic.NewAtomicInt(0),
	}

	go ds.startFetching()

	return ds, nil
}
