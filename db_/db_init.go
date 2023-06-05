package db_

import (
	"database/sql"
	"fmt"
	"github.com/xiaoqinglee/golang-sql-demo/config"
	"log"
)

func GetDB() (connectionPool *sql.DB, cleanup func()) {
	connStr := fmt.Sprintf(
		"host=%v "+
			"port=%v "+
			"dbname=%v "+
			"search_path=%v "+
			"user=%v "+
			"sslmode=%v ",
		config.DemoProjectDB["host"],
		config.DemoProjectDB["port"],
		config.DemoProjectDB["dbname"],
		config.DemoProjectDB["search_path"],
		config.DemoProjectDB["user"],
		config.DemoProjectDB["sslmode"],
	)
	connectionPool, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	//test conn reuse
	connectionPool.SetMaxOpenConns(10)

	err = connectionPool.Ping()
	if err != nil {
		log.Fatal(err)
	}
	return connectionPool, func() {
		_ = connectionPool.Close()
	}
}
