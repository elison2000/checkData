package util

import (
    "database/sql"
    "fmt"
    _ "github.com/lib/pq"
    "strings"
    "time"
)

type PgsqlDb struct {
    Host     string
    Port     int
    User     string
    Password string
    Database string
    ConnPool *sql.DB
    Query    func(query string, args ...any) (*sql.Rows, error)
}

func (self *PgsqlDb) Init() (err error) {
    //获取数据库连接
    dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", self.User, self.Password, self.Host, self.Port, self.Database)
    db, err := sql.Open("postgres", dsn)
    if err != nil {
        return
    }
    self.ConnPool = db
    self.ConnPool.SetMaxOpenConns(100)                 //最大连接数
    self.ConnPool.SetMaxIdleConns(10)                  //连接池里最大空闲连接数。必须要比maxOpenConns小
    self.ConnPool.SetConnMaxLifetime(time.Second * 10) //最大存活保持时间
    self.ConnPool.SetConnMaxIdleTime(time.Second * 10) //最大空闲保持时间
    self.Query = self.ConnPool.Query
    return
}

func (self *PgsqlDb) QueryReturnList(sqltext string) ([][]string, error) {
    //执行sql，返回二维数组
    cur, err := self.ConnPool.Query(sqltext)
    if err != nil {
        return nil, err
    }
    defer cur.Close()

    cols, err := cur.Columns()
    if err != nil {
        return nil, err
    }
    row := make([][]byte, len(cols))
    rowP := make([]interface{}, len(cols))
    data := [][]string{}

    for i := range row {
        rowP[i] = &row[i]
    }
    for cur.Next() {
        err := cur.Scan(rowP...)
        if err != nil {
            return nil, err
        }
        rowSlice := make([]string, len(cols)) //不能在循环外层定义，否则是浅拷贝
        for i, v := range row {
            rowSlice[i] = string(v)
        }

        data = append(data, rowSlice)
    }
    return data, nil
}

func (self *PgsqlDb) QueryReturnText(sqltext string) ([]string, error) {
    //执行sql，返回二维数组
    // 查询数据
    cur, err := self.ConnPool.Query(sqltext)
    if err != nil {
        return []string{}, err
    }
    defer cur.Close()

    // 获取列名
    columns, err := cur.Columns()
    if err != nil {
        return []string{}, err
    }

    // 生成sql语句
    data := make([]string, 0)
    for cur.Next() {
        row := make([]*sql.NullString, len(columns))
        rowP := make([]interface{}, len(columns))
        for i, _ := range row {
            rowP[i] = &row[i]
        }

        // 读取数据
        if err := cur.Scan(rowP...); err != nil {
            return []string{}, err
        }

        rowText := make([]string, len(columns))

        for key, value := range row {
            if value != nil && value.Valid {
                //rowText[key] = strings.Replace(rowText[key], `\`, `\\`, -1)
                rowText[key] = strings.Replace(rowText[key], `'`, `''`, -1)
                rowText[key] = "'" + value.String + "'"
            } else {
                rowText[key] = "null"
            }
        }

        data = append(data, "("+strings.Join(rowText, ",")+")")
    }
    return data, nil
}

func (self *PgsqlDb) QueryReturnDict(sqltext string) ([]map[string]string, error) {
    //执行sql，返回二维map
    cur, err := self.ConnPool.Query(sqltext)
    if err != nil {
        return nil, err
    }
    defer cur.Close()

    cols, err := cur.Columns()
    if err != nil {
        return nil, err
    }
    result := make([][]byte, len(cols))
    resultP := make([]interface{}, len(cols))
    data := []map[string]string{}

    for i := range result {
        resultP[i] = &result[i]
    }
    for cur.Next() {
        err := cur.Scan(resultP...)
        if err != nil {
            return nil, err
        }
        row := make(map[string]string, len(cols)) //在循环内创建内存，才是深拷贝模式
        for i, v := range result {
            row[cols[i]] = string(v)
        }

        data = append(data, row)
    }
    return data, nil
}

func (self *PgsqlDb) Close() {
    self.ConnPool.Close()
}
