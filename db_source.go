package main

import (
	"fmt"

	"github.com/integration-system/isp-lib/v2/structure"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
)

const (
	cursorName = "mqpusher_cursor"
	batchSize  = 5000
)

type DbDataSource struct {
	cfg    DBSource
	errCh  chan error
	rowsCh chan map[string]interface{}
	db     *sqlx.DB
}

func (s *DbDataSource) GetRow() (map[string]interface{}, error) {
	select {
	case err := <-s.errCh:
		return nil, err
	case row, open := <-s.rowsCh:
		if !open {
			return nil, nil
		}
		return row, nil
	}
}

func (s *DbDataSource) Close() error {
	return s.db.Close()
}

func (s *DbDataSource) fetchData() {
	err := func() (err error) {
		tx, err := s.db.Beginx()
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			} else {
				_ = tx.Commit()
			}
		}()

		rows, err := tx.Queryx(s.cfg.Query)
		if err != nil {
			return err
		}

		for rows.Next() {
			row := make(map[string]interface{})
			err = rows.MapScan(row)
			if err != nil {
				return err
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
		tx, err := s.db.Beginx()
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			} else {
				_ = tx.Commit()
			}
		}()

		_, err = tx.Exec(fmt.Sprintf("DECLARE %s CURSOR FOR %s", cursorName, s.cfg.Query))
		if err != nil {
			return err
		}

		for {
			rows, err := tx.Queryx(fmt.Sprintf("FETCH %d FROM %s", batchSize, cursorName))
			if err != nil {
				return err
			}

			count := 0
			for rows.Next() {
				row := make(map[string]interface{})
				err = rows.MapScan(row)
				if err != nil {
					return err
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
	db, err := sqlx.Connect("pgx", sqlConnString(cfg.Database))

	if err != nil {
		return nil, err
	}

	ds := &DbDataSource{
		cfg:    cfg,
		db:     db,
		rowsCh: make(chan map[string]interface{}),
		errCh:  make(chan error),
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
