package main

import (
	"database/sql"
	"encoding/json"
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
	ReportContent  null.String `db:"report_content"`
	ReportedAt     time.Time   `db:"reported_at"`
	UnmappedInDest int64
}

type ReportV3 struct {
	ReportId       int64       `db:"report_id"`
	Reporter       int64       `db:"reporter"`
	ReportContent  zero.String `db:"report_content"`
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
	testInClauseAndNamedQuery()
}
