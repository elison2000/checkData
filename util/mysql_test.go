package util

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"testing"
	"time"
)

func TestMysqlDb_QueryReturnList(t *testing.T) {
	db := &MysqlDb{Host: "192.168.1.204", Port: 3307, User: "root", Password: "123456", Database: "test1"}
	db.Init()
	sql := "select `id`, `status` from `allocation_command` limit 5"
	res, err := db.QueryReturnList(sql)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}

func TestMysqlDb_QueryReturnDict(t *testing.T) {
	db := &MysqlDb{Host: "192.168.1.204", Port: 3307, User: "root", Password: "123456", Database: "test1"}
	db.Init()
	sql := "select `id`, `status` from `allocation_command` limit 5"
	res, err := db.QueryReturnDict(sql)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}

//func TestMysqlDb_QueryReturnText(t *testing.T) {
//	db := &MysqlDb{Host: "192.168.1.204", Port: 3307, User: "root", Password: "123456", Database: "test1"}
//	db.Init()
//	sql := "select `id`, `status` from `allocation_command` limit 5"
//	res, err := db.QueryReturnText(sql)
//	if err != nil {
//		fmt.Println(err)
//	}
//	fmt.Println(res)
//}

func TestMysqlDb_QueryReturnText(t *testing.T) {
	db := &MysqlDb{Host: "192.168.1.204", Port: 3307, User: "root", Password: "123456", Database: "test1"}
	db.Init()
	sql := "select `id`, `status` from `allocation_command` limit 10"
	cur, err := db.ConnPool.Query(sql)
	if err != nil {
		fmt.Println("11: ", err)
	}

	for cur.Next() {
		time.Sleep(time.Second * 30)
	}

}
