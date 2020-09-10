package source

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/integration-system/isp-lib/v2/structure"
	"github.com/integration-system/mqpusher/conf"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/test_driver"
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

var (
	sqlParser    *parser.Parser
	modWhereExpr *ast.BinaryOperationExpr
	parserLock   sync.Mutex
)

func init() {
	const query = `SELECT id from something WHERE MOD(id, 200) = 100`
	sqlParser = parser.New()
	stmt, err := sqlParser.ParseOneStmt(query, "", "")
	if err != nil {
		panic(err)
	}

	modWhereExpr = stmt.(*ast.SelectStmt).Where.(*ast.BinaryOperationExpr)
}

type DbDataSource struct {
	cfg           conf.DBSource
	errCh         chan error
	rowsCh        chan map[string]interface{}
	ctx           context.Context
	cancel        func()
	db            *pgxpool.Pool
	totalRows     int64
	processedRows int64
}

func (s *DbDataSource) GetData() (interface{}, error) {
	select {
	case err := <-s.errCh:
		s.cancel()
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
	s.cancel()
	s.db.Close()
	return nil
}

func (s *DbDataSource) fetchDataCursor(total, number int) {
	err := func() (err error) {
		conn, err := s.db.Acquire(s.ctx)
		if err != nil {
			return err
		}
		defer conn.Release()
		RegisterTypes(conn.Conn().ConnInfo())

		tx, err := conn.Begin(s.ctx)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback(s.ctx)
			} else {
				_ = tx.Commit(s.ctx)
			}
		}()

		query := s.cfg.Query
		cursorNameN := fmt.Sprintf("%s_%d", cursorName, number)
		if total > 1 {
			query, err = AppendIDModClauseToQuery(query, total, number)
			if err != nil {
				return err
			}
		}

		_, err = tx.Exec(s.ctx, fmt.Sprintf("DECLARE %s CURSOR FOR %s", cursorNameN, query))
		if err != nil {
			return err
		}

		for {
			rows, err := tx.Query(s.ctx, fmt.Sprintf("FETCH %d FROM %s", batchSize, cursorNameN))
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
		select {
		case s.errCh <- err:
		default:

		}
	}
}

func (s *DbDataSource) startFetching(parallel int) {
	wg := new(sync.WaitGroup)
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func(number int) {
			defer wg.Done()
			s.fetchDataCursor(parallel, number)
		}(i)
	}
	wg.Wait()
	close(s.rowsCh)
}

func NewDbDataSource(cfg conf.DBSource) (ds DataSource, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		if err != nil {
			cancel()
		}
	}()
	db, err := pgxpool.Connect(ctx, sqlConnString(cfg.Database))
	if err != nil {
		return nil, err
	}

	totalRows, err := EstimateQueryTotalRows(db, cfg.Query)
	if err != nil {
		return nil, err
	}

	parallel := 1
	if cfg.Parallel > 0 {
		parallel = cfg.Parallel
	}
	dbDs := &DbDataSource{
		cfg:       cfg,
		db:        db,
		totalRows: totalRows,
		rowsCh:    make(chan map[string]interface{}, batchSize*parallel),
		errCh:     make(chan error, 1),
		ctx:       ctx,
		cancel:    cancel,
	}
	go dbDs.startFetching(parallel)

	return dbDs, nil
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

	query = strings.ReplaceAll(query, "'", "''")
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

func AppendIDModClauseToQuery(query string, idMod, equalsTo int) (string, error) {
	parserLock.Lock()
	defer parserLock.Unlock()

	stmt, err := sqlParser.ParseOneStmt(query, "", "")
	if err != nil {
		return "", fmt.Errorf("can't parse sql query to append where clause: %v", err)
	}
	selectStmt := stmt.(*ast.SelectStmt)

	// 200 from example query
	modOperand := modWhereExpr.L.(*ast.BinaryOperationExpr).R.(*test_driver.ValueExpr)
	modOperand.SetInt64(int64(idMod))

	// 100 from example query
	modResultOperand := modWhereExpr.R.(*test_driver.ValueExpr)
	modResultOperand.SetInt64(int64(equalsTo))

	if selectStmt.Where == nil {
		selectStmt.Where = modWhereExpr
	} else {
		newWhereClause := &ast.BinaryOperationExpr{
			Op: opcode.LogicAnd,
			L:  selectStmt.Where,
			R:  modWhereExpr,
		}
		selectStmt.Where = newWhereClause
	}

	var sb strings.Builder
	const formatFlags = format.RestoreStringSingleQuotes | format.RestoreKeyWordUppercase |
		format.RestoreSpacesAroundBinaryOperation
	err = selectStmt.Restore(format.NewRestoreCtx(formatFlags, &sb))
	if err != nil {
		return "", fmt.Errorf("can't print modified sql query: %v", err)
	}

	return sb.String(), nil
}
