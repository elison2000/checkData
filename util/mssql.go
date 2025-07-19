package util

import (
	"database/sql"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"time"
)

func NewMssqlDB(host string, port int, user, password, database string) (db *sql.DB, err error) {
	//获取数据库连接
	dsn := fmt.Sprintf("server=%s,%d;user id=%s;password=%s;database=%s;encrypt=disable", host, port, user, password, database)
	db, err = sql.Open("sqlserver", dsn)
	if err != nil {
		return
	}
	db.SetMaxOpenConns(64)                    //最大连接数
	db.SetMaxIdleConns(32)                    //连接池里最大空闲连接数。必须要比maxOpenConns小
	db.SetConnMaxLifetime(time.Second * 3600) //最大存活保持时间
	db.SetConnMaxIdleTime(time.Second * 3600) //最大空闲保持时间
	return
}
