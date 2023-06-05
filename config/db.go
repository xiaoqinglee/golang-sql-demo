package config

import _ "github.com/lib/pq"

var DemoProjectDB = map[string]string{
	"host":        "localhost",
	"port":        "5432",
	"dbname":      "postgres",
	"search_path": "im",
	"user":        "postgres",
	"password":    "",
	"sslmode":     "disable",
}
