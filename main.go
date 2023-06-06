package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/k0kubun/pp/v3"
	"github.com/xiaoqinglee/golang-sql-demo/db_"
	"gopkg.in/guregu/null.v4"
	"gopkg.in/guregu/null.v4/zero"
)

func testConnectionReuse() {
	db, cleanup := db_.GetDB()
	defer cleanup()
	var timeVal time.Time
	pp.Println(timeVal)

	//如果 QueryRow 传入的 sql 执行结果返回了多行, 那么客户端不会报错, Scan()动作只Scan其中一行.
	err := db.QueryRow("select reported_at from report_t where report_id = 2").Scan(&timeVal)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(timeVal)

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	var oldTimeZoneForThisSession string
	err = tx.QueryRow("show timezone").Scan(&oldTimeZoneForThisSession)
	if err != nil {
		log.Fatal(err)
	}
	pp.Println("oldTimeZoneForThisSession", oldTimeZoneForThisSession)
	_, err = tx.Exec("set timezone = 'Asia/Tokyo'")
	if err != nil {
		log.Fatal(err)
	}
	err = tx.QueryRow("select reported_at from report_t where report_id = 2").Scan(&timeVal)
	if err != nil {
		log.Fatal(err)
	}

	//http://go-database-sql.org/modifying.html
	// If you need to work with multiple statements that modify connection state,
	//you need a Tx even if you don’t want a transaction per se.
	// 如果某个 sql 改变了数据库连接的状态, 那么将连接归还到数据库的时候要恢复原来的状态.

	// http://go-database-sql.org/surprises.html
	// Connection State Mismatch
	// Additionally, after you’ve changed the connection,
	//it’ll return to the pool and potentially pollute the state for some other code.
	//This is one of the reasons why you should never issue BEGIN or COMMIT statements as SQL commands directly, too.

	_, err = tx.Exec(fmt.Sprintf("set timezone = '%v'", oldTimeZoneForThisSession))
	if err != nil {
		log.Fatal(err)
	}
	tx.Commit()
	pp.Println(timeVal)

	for i := 0; i < 100; i++ {
		go func() {
			err = db.QueryRow("select reported_at from report_t where report_id = 2").Scan(&timeVal)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(timeVal)
		}()
	}
	time.Sleep(10 * time.Second)
}

type Report struct {
	ReportId       int64          `db:"report_id"`
	Reporter       int64          `db:"reporter"`
	ReportContent  sql.NullString `db:"report_content"`
	ReportedAt     time.Time      `db:"reported_at"`
	UnmappedInDest int64
}

func testManuallyScanAndAutoScan() {
	sqlDb, cleanup := db_.GetDB()
	defer cleanup()
	db := sqlx.NewDb(sqlDb, "postgres")

	rows, err := db.Queryx("SELECT * FROM report_t")
	for rows.Next() {
		oneRowData := make(map[string]interface{})
		err = rows.MapScan(oneRowData)
		if err != nil {
			pp.Println(err)
		} else {
			pp.Println(oneRowData)
		}
		oneRowData2, err := rows.SliceScan()
		if err != nil {
			pp.Println(err)
		} else {
			pp.Println(oneRowData2)
		}
	}

	var reports []*Report
	err = db.Select(&reports, "SELECT * FROM report_t")
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println(reports)
	}
}

func testInClauseAndNamedQuery() {
	sqlDb, cleanup := db_.GetDB()
	defer cleanup()
	db := sqlx.NewDb(sqlDb, "postgres")

	//"=================test in================"
	//"ids" []int{
	//  1,
	//  2,
	//  3,
	//}
	//"args" []interface {}{
	//  1,
	//  2,
	//  3,
	//}
	//"query" "SELECT * FROM report_t WHERE report_id IN (?, ?, ?)"
	//"query" "SELECT * FROM report_t WHERE report_id IN ($1, $2, $3)"
	//[]*main.Report{
	//  &main.Report{
	//    ReportId:      1,
	//    Reporter:      1,
	//    ReportContent: sql.NullString{
	//      String: "",
	//      Valid:  false,
	//    },
	//    ReportedAt:     2023-06-05 15:50:21 Asia/Shanghai,
	//    UnmappedInDest: 0,
	//  },
	//  &main.Report{
	//    ReportId:      2,
	//    Reporter:      2,
	//    ReportContent: sql.NullString{
	//      String: "",
	//      Valid:  false,
	//    },
	//    ReportedAt:     2023-06-05 15:50:21 Asia/Shanghai,
	//    UnmappedInDest: 0,
	//  },
	//  &main.Report{
	//    ReportId:      3,
	//    Reporter:      3,
	//    ReportContent: sql.NullString{
	//      String: "",
	//      Valid:  false,
	//    },
	//    ReportedAt:     2023-06-05 15:50:21 Asia/Shanghai,
	//    UnmappedInDest: 0,
	//  },
	//}
	pp.Println("=================test in================")
	var reports []*Report
	var ids = []int{1, 2, 3}
	query, args, err := sqlx.In("SELECT * FROM report_t WHERE report_id IN (?)", ids)
	if err != nil {
		pp.Println(err)
	}
	pp.Println("ids", ids)
	pp.Println("args", args)
	pp.Println("query", query)
	query = db.Rebind(query)
	pp.Println("query", query)
	err = db.Select(&reports, query, args...)
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println(reports)
	}

	//"================test named query================="
	//"args" []interface {}{
	//  42,
	//}
	//"query" "SELECT * FROM report_t WHERE reporter=?"
	//"query" "SELECT * FROM report_t WHERE reporter=$1"
	//[]*main.Report(nil)
	pp.Println("================test named query=================")
	var reports2 []*Report
	arg := map[string]interface{}{
		"reported_by": 42,
	}
	query, args, err = sqlx.Named("SELECT * FROM report_t WHERE reporter=:reported_by", arg)
	pp.Println("args", args)
	pp.Println("query", query)
	query = db.Rebind(query)
	pp.Println("query", query)
	err = db.Select(&reports2, query, args...)
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println(reports2)
	}

	//"===============together=================="
	//"args" []interface {}{
	//  42,
	//  []int{
	//    1,
	//    2,
	//    3,
	//  },
	//}
	//"query" "SELECT * FROM report_t WHERE reporter=? AND report_id IN (?)"
	//"args" []interface {}{
	//  42,
	//  1,
	//  2,
	//  3,
	//}
	//"query" "SELECT * FROM report_t WHERE reporter=? AND report_id IN (?, ?, ?)"
	//"query" "SELECT * FROM report_t WHERE reporter=$1 AND report_id IN ($2, $3, $4)"
	//[]*main.Report(nil)
	pp.Println("===============together==================")
	var reports3 []*Report
	arg = map[string]interface{}{
		"ids":         []int{1, 2, 3},
		"reported_by": 42,
	}
	query, args, err = sqlx.Named("SELECT * FROM report_t WHERE reporter=:reported_by AND report_id IN (:ids)", arg)
	pp.Println("args", args)
	pp.Println("query", query)
	query, args, err = sqlx.In(query, args...)
	pp.Println("args", args)
	pp.Println("query", query)
	query = db.Rebind(query)
	pp.Println("query", query)
	err = db.Select(&reports3, query, args...)
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println(reports3)
	}
}

func testUnsafeVersionDB() {
	sqlDb, cleanup := db_.GetDB()
	defer cleanup()
	db := sqlx.NewDb(sqlDb, "postgres")
	var report Report
	err := db.Get(&report, "SELECT *, 42 unmapped_in_src FROM report_t LIMIT 1")
	if err != nil {
		pp.Println(err) // missing destination name unmapped_in_src in *main.Report
	} else {
		pp.Println(report)
	}

	udb := db.Unsafe()
	err = udb.Get(&report, "SELECT *, 42 unmapped_in_src FROM report_t LIMIT 1")
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println(report) //ok
	}
	err = udb.Get(&report, "SELECT *, 42 unmapped_in_src FROM report_t LIMIT 1")
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println(report) //ok
	}
}

type ReportV2 struct {
	ReportId       int64       `db:"report_id"`
	Reporter       int64       `db:"reporter"`
	ReportContent  null.String `db:"report_content"` // 区分 null 和 zero value
	ReportedAt     time.Time   `db:"reported_at"`
	UnmappedInDest int64
}

type ReportV3 struct {
	ReportId       int64       `db:"report_id"`
	Reporter       int64       `db:"reporter"`
	ReportContent  zero.String `db:"report_content"` // 不区分 null 和 zero value
	ReportedAt     time.Time   `db:"reported_at"`
	UnmappedInDest int64
}

func testMarshallAndUnmarshallNullableFields() {
	sqlDb, cleanup := db_.GetDB()
	defer cleanup()
	db := sqlx.NewDb(sqlDb, "postgres")

	reportV2 := &ReportV2{}
	err := db.Get(reportV2, "SELECT * FROM report_t limit 1")
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println(reportV2)
	}
	reportV3 := &ReportV3{}
	err = db.Get(reportV3, "SELECT * FROM report_t limit 1")
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println(reportV3)
	}

	bytes_, err := json.Marshal(reportV2)
	fmt.Println(string(bytes_), err) //{"ReportId":1,"Reporter":1,"ReportContent":null,"ReportedAt":"2023-06-05T15:50:21.140872+08:00","UnmappedInDest":0} <nil>
	reportV2 = &ReportV2{}
	err = json.Unmarshal(bytes_, reportV2)
	pp.Println(reportV2, err)

	bytes_, err = json.Marshal(reportV3)
	fmt.Println(string(bytes_), err) //{"ReportId":1,"Reporter":1,"ReportContent":"","ReportedAt":"2023-06-05T15:50:21.140872+08:00","UnmappedInDest":0} <nil>
	reportV3 = &ReportV3{}
	err = json.Unmarshal(bytes_, reportV3)
	pp.Println(reportV3, err)
}

func main() {
	testTxnPropagation()
}

type ISqlxConn interface {
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	Rebind(query string) string
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

type ISqlxTx interface {
	ISqlxDBOrSqlxTx
	NamedStmt(stmt *sqlx.NamedStmt) *sqlx.NamedStmt
	NamedStmtContext(ctx context.Context, stmt *sqlx.NamedStmt) *sqlx.NamedStmt
	Stmtx(stmt interface{}) *sqlx.Stmt
	StmtxContext(ctx context.Context, stmt interface{}) *sqlx.Stmt
	Unsafe() *sqlx.Tx
}

type ISqlxDB interface {
	ISqlxDBOrSqlxTx
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
	Beginx() (*sqlx.Tx, error)
	Connx(ctx context.Context) (*sqlx.Conn, error)
	MapperFunc(mf func(string) string)
	MustBegin() *sqlx.Tx
	MustBeginTx(ctx context.Context, opts *sql.TxOptions) *sqlx.Tx
	NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error)
	Unsafe() *sqlx.DB
}

type ISqlxDBOrSqlxTx interface {
	BindNamed(query string, arg interface{}) (string, []interface{}, error)
	DriverName() string
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	MustExec(query string, args ...interface{}) sql.Result
	MustExecContext(ctx context.Context, query string, args ...interface{}) sql.Result
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
	PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)
	Preparex(query string) (*sqlx.Stmt, error)
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	Rebind(query string) string
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

// 在初始化 connectionPool的时候就将 sqlx.DB 设置为 Unsafe, 这样, 所有由这个对象生成出来的对象都是 unsafe 的.

// 每个业务方法的事务传播策略默认为 TxSupports:
// 如果 inputHandle 是事务就在事务中执行, 如果 inputHandle 不是事务就用连接池.
// 使用这种事务传播策略时什么都不必额外做.
//
// 还有一种事务传播策略 TxRequired:
// 如果 inputHandle 是事务就在事务中执行, 如果 inputHandle 不是事务, 那么就创建一个事务.
// 如果事务是当前方法创建的, 那么离开当前方法时会根据情况决定提交或回滚次事务.
// TxRequired 的调用和 conditionallyCommitFunc 的调用应该应该在一个函数中成对.

func TxRequired(inputHandle ISqlxDBOrSqlxTx) (outputHandle ISqlxDBOrSqlxTx, conditionallyCommitFunc func(originalErrorWhenLeaveTxScope error) (finalError error), err error) {
	switch inputHandle := inputHandle.(type) {
	case *sqlx.Tx:
		noOp := func(originalErrorWhenLeaveTxScope error) (finalError error) { return originalErrorWhenLeaveTxScope }
		return inputHandle, noOp, nil
	case *sqlx.DB:
		sqlxTx, err := inputHandle.Beginx()
		if err != nil {
			return nil, nil, err
		}
		commitOrRollback := func(originalErrorWhenLeaveTxScope error) (finalError error) {
			if originalErrorWhenLeaveTxScope != nil {
				rollbackError := sqlxTx.Rollback()
				if rollbackError != nil {
					return errors.Join(originalErrorWhenLeaveTxScope, rollbackError)
				}
				return originalErrorWhenLeaveTxScope
			}
			commitError := sqlxTx.Commit()
			return commitError
		}
		return sqlxTx, commitOrRollback, nil
	default:
		return nil, nil, fmt.Errorf("invalid inputHandle: %v", inputHandle)
	}
}

func testTxnPropagation() {
	sqlDb, cleanup := db_.GetDB()
	defer cleanup()
	db := sqlx.NewDb(sqlDb, "postgres")
	db = db.Unsafe()
	o := &Outer{&Mid{&Inner{}}}

	err := o.needTx(db)
	if err != nil {
		panic(err)
	}
}

type Outer struct{ *Mid }
type Mid struct{ *Inner }
type Inner struct{}

func (o *Outer) needTx(tx ISqlxDBOrSqlxTx) (err error) {
	tx, conditionallyCommitFunc, err := TxRequired(tx)
	if err != nil {
		return err
	}
	defer func() {
		err = conditionallyCommitFunc(err)
	}()
	err = o.Mid.needTx(tx)
	if err != nil {
		return err
	}
	report := &Report{}
	err = tx.Get(report, "SELECT * FROM report_t where report_id in (42)")
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println("outer needTx:", report)
	}
	err = o.Mid.notNeedTx(tx)
	if err != nil {
		return err
	}
	time.Sleep(time.Minute * 5)
	return nil
}

func (m *Mid) needTx(tx ISqlxDBOrSqlxTx) (err error) {
	tx, conditionallyCommitFunc, err := TxRequired(tx)
	if err != nil {
		return err
	}
	defer func() {
		err = conditionallyCommitFunc(err)
	}()
	err = m.Inner.needTx(tx)
	if err != nil {
		return err
	}
	report := &Report{}
	err = tx.Get(report, "SELECT * FROM report_t where report_id in (42)")
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println("mid needTx:", report)
	}
	return nil
}

func (m *Mid) notNeedTx(db ISqlxDBOrSqlxTx) (err error) {
	report := &Report{}
	err = db.Get(report, "SELECT * FROM report_t where report_id in (42)")
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println("mid notNeedTx:", report)
	}
	return nil
}

func (i *Inner) needTx(tx ISqlxDBOrSqlxTx) (err error) {
	tx, conditionallyCommitFunc, err := TxRequired(tx)
	if err != nil {
		return err
	}
	defer func() {
		err = conditionallyCommitFunc(err)
	}()
	report := &Report{}
	err = tx.Get(report, "update report_t set report_id=42 where report_id in (9) returning *")
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println("inner needTx update:", report)
	}
	if err != nil {
		return err
	}
	report2 := &Report{}
	err = tx.Get(report2, "SELECT * FROM report_t where report_id in (42)")
	if err != nil {
		pp.Println(err)
	} else {
		pp.Println("inner needTx get:", report2)
	}
	return nil
}
