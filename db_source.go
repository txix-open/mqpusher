package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/integration-system/isp-lib/v2/structure"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	cursorName = "mqpusher_cursor"
	batchSize  = 5000
)

const countRowsFunc = `
DO $$
    DECLARE
        plan jsonb;
    BEGIN
        EXECUTE 'EXPLAIN (FORMAT JSON) %s' INTO plan;
        CREATE TEMPORARY TABLE __table ON COMMIT DROP AS SELECT (plan -> 0 -> 'Plan' ->> 'Plan Rows')::bigint;
    END
$$;
`
const countRowsQuery = "SELECT * from __table;"

type DbDataSource struct {
	cfg           DBSource
	errCh         chan error
	rowsCh        chan map[string]interface{}
	db            *pgxpool.Pool
	totalRows     int64
	processedRows int64
}

func (s *DbDataSource) GetData() (interface{}, error) {
	select {
	case err := <-s.errCh:
		return nil, err
	case row, open := <-s.rowsCh:
		if !open {
			return nil, nil
		}
		atomic.AddInt64(&s.processedRows, 1)
		return row, nil
	}
}

func (s *DbDataSource) Progress() (int64, float32) {
	current := atomic.LoadInt64(&s.processedRows)
	return current, float32(current) / float32(s.totalRows) * 100
}

func (s *DbDataSource) Close() error {
	s.db.Close()
	return nil
}

func (s *DbDataSource) fetchData() {
	err := func() (err error) {
		ctx := context.Background()
		conn, err := s.db.Acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()
		RegisterTypes(conn.Conn().ConnInfo())

		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback(ctx)
			} else {
				_ = tx.Commit(ctx)
			}
		}()

		rows, err := tx.Query(ctx, s.cfg.Query)
		if err != nil {
			return err
		}

		var columns []string
		for rows.Next() {
			vals, err := rows.Values()
			if columns == nil {
				fieldDescriptions := rows.FieldDescriptions()
				columns = make([]string, len(fieldDescriptions))
				for i := range fieldDescriptions {
					columns[i] = string(fieldDescriptions[i].Name)
				}
			}

			if err != nil {
				return err
			}
			row := make(map[string]interface{}, len(vals))
			for i := range columns {
				row[columns[i]] = vals[i]
			}

			s.rowsCh <- row
		}

		err = rows.Err()
		if err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		s.errCh <- err
	}
	close(s.rowsCh)
}

func (s *DbDataSource) fetchDataCursor() {
	err := func() (err error) {
		ctx := context.Background()
		conn, err := s.db.Acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()
		RegisterTypes(conn.Conn().ConnInfo())

		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback(ctx)
			} else {
				_ = tx.Commit(ctx)
			}
		}()

		_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR FOR %s", cursorName, s.cfg.Query))
		if err != nil {
			return err
		}

		for {
			rows, err := tx.Query(ctx, fmt.Sprintf("FETCH %d FROM %s", batchSize, cursorName))
			if err != nil {
				return err
			}

			var columns []string
			count := 0
			for rows.Next() {
				vals, err := rows.Values()
				if columns == nil {
					fieldDescriptions := rows.FieldDescriptions()
					columns = make([]string, len(fieldDescriptions))
					for i := range fieldDescriptions {
						columns[i] = string(fieldDescriptions[i].Name)
					}
				}

				if err != nil {
					return err
				}
				row := make(map[string]interface{}, len(vals))
				for i := range columns {
					row[columns[i]] = vals[i]
				}

				s.rowsCh <- row
				count++
			}

			err = rows.Err()
			if err != nil {
				return err
			}
			if count == 0 {
				break
			}
		}
		return nil
	}()

	if err != nil {
		s.errCh <- err
	}
	close(s.rowsCh)
}

func NewDbDataSource(cfg DBSource) (DataSource, error) {
	db, err := pgxpool.Connect(context.Background(), sqlConnString(cfg.Database))
	if err != nil {
		return nil, err
	}

	totalRows, err := EstimateQueryTotalRows(db, cfg.Query)
	if err != nil {
		return nil, err
	}

	ds := &DbDataSource{
		cfg:       cfg,
		db:        db,
		totalRows: totalRows,
		rowsCh:    make(chan map[string]interface{}),
		errCh:     make(chan error),
	}

	if ds.cfg.Cursor {
		go ds.fetchDataCursor()
	} else {
		go ds.fetchData()
	}

	return ds, nil
}

func sqlConnString(config structure.DBConfiguration) string {
	cs := fmt.Sprintf(
		"postgres://%s:%s/%s?search_path=%s,public&sslmode=disable&user=%s&password=%s",
		config.Address,
		config.Port,
		config.Database,
		config.Schema,
		config.Username,
		config.Password,
	)

	return cs
}

func EstimateQueryTotalRows(db *pgxpool.Pool, query string) (int64, error) {
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Commit(ctx)
	}()

	_, err = tx.Exec(ctx, fmt.Sprintf(countRowsFunc, query))
	if err != nil {
		return 0, err
	}

	row := tx.QueryRow(ctx, countRowsQuery)
	var count int64
	err = row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
