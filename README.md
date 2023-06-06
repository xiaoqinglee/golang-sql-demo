# golang-sql-demo


database/sql:

https://pkg.go.dev/database/sql

http://go-database-sql.org/

http://go-database-sql.org/surprises.html


sqlx:

https://github.com/jmoiron/sqlx

https://pkg.go.dev/github.com/jmoiron/sqlx

http://jmoiron.github.io/sqlx/

http://jmoiron.github.io/sqlx/#connectionPool


squirrel:

https://github.com/Masterminds/squirrel


lib/pq:

https://github.com/lib/pq

https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

http://go-database-sql.org/errors.html

https://github.com/lib/pq/blob/master/error.go

https://www.postgresql.org/docs/current/errcodes-appendix.html


null.v4

https://github.com/guregu/null

https://stackoverflow.com/questions/33072172/how-can-i-work-with-sql-null-values-and-json-in-a-good-way


Summary:

To interact with data that persists in relational databases, three abilities listed below can make us productive: 

i. To dynamically build query strings at runtime in a programmatic way. 

Concatenating strings is a pain. 

ii. To execute static raw sqls. 

Raw sqls are more concise and straightforward than programmatically built ones if the concrete query strings can be completely known before the program runs. 
Sql language is too expressive and sometimes sql builders are unable to build sqls as expressive as raw sqls. 

iii. To map between a database row and a program object automatically. 

This saves us labor.


高级话题:

事务传播: https://www.liaoxuefeng.com/wiki/1252599548343744/1282383642886177
