package util

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"testing"
)

func TestPgsqlDb_QueryReturnList(t *testing.T) {
	db := &PgsqlDb{Host: "172.16.11.31", Port: 5432, User: "postgres", Password: "postgres", Database: "test1"}
	db.Init()
	sql := "select ebayid,title from sp_ebay.ebay limit 3"
	res, err := db.QueryReturnList(sql)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}

func TestPgsqlDb_QueryReturnDict(t *testing.T) {
	db := &PgsqlDb{Host: "172.16.11.31", Port: 5432, User: "postgres", Password: "postgres", Database: "test1"}
	db.Init()
	sql := "select ebayid,title from sp_ebay.ebay limit 3"
	res, err := db.QueryReturnDict(sql)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}

func TestPgsqlDb_QueryReturnText(t *testing.T) {
	db := &PgsqlDb{Host: "172.16.11.31", Port: 5432, User: "postgres", Password: "postgres", Database: "test1"}
	db.Init()
	sql := "select ebayid,title from sp_ebay.ebay limit 3"
	res, err := db.QueryReturnText(sql)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}
