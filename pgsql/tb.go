package pgsql

import (
    "checkData/model"
    "checkData/util"
    "fmt"
    "github.com/gookit/slog"
    "strconv"
    "strings"
)

type Table struct {
    DbName      string
    TbName      string
    Mode        string //fast,slow,count
    Keys        []string
    Columns     []string
    ColumnTypes []string
    Where       string
    SkipColumns []string
    SqlText     string
    DbGroup     *Database
    Result      *model.Result
}

func NewTable(tb string, dbg *Database, opt *model.Options) *Table {
    return &Table{
        DbName:      dbg.TargetDb,
        TbName:      tb,
        Mode:        opt.Mode,
        SkipColumns: opt.SkipColList,
        Keys:        opt.KeysList,
        Where:       opt.Where,
        DbGroup:     dbg,
        Result:      &model.Result{DbName: dbg.TargetDb, TbName: tb, RecheckPassRows: -1},
    }
}

func (self *Table) GetDbName() string {
    return self.DbName
}

func (self *Table) GetTbName() string {
    return self.TbName
}

func (self *Table) splitTableName() (string, string) {
    //拆分列名
    l := strings.Split(self.TbName, `.`)
    schema := l[0]
    tb := l[1]
    return schema, tb
}

func (self *Table) getTableColumns() error {
    // 获取表列名
    pk := []string{}
    cols := []string{}
    skipcols := []string{}
    schema, tb := self.splitTableName()
    sql1 := fmt.Sprintf(`select a.attname
from pg_class c
join pg_attribute a on a.attrelid = c.oid
join pg_namespace n on n.oid = c.relnamespace
where a.attnum > 0 and n.nspname='%s' and c.relname = '%s'
order by a.attnum`, schema, tb)
    sql2 := fmt.Sprintf(`select pg_attribute.attname as column_name,pg_class.relname
from pg_index, pg_class, pg_attribute, pg_namespace
where pg_namespace.oid = pg_class.relnamespace and pg_namespace.nspname = '%s' and pg_class.relname='%s' and indrelid = pg_class.oid and pg_attribute.attrelid = pg_class.oid and pg_attribute.attnum = any(pg_index.indkey) and indisprimary
order by array_position(pg_index.indkey, pg_attribute.attnum)`, schema, tb)

    //获取列名
    rows1, err1 := self.DbGroup.SourceDbConn.QueryReturnList(sql1)
    if err1 != nil {
        return fmt.Errorf("getTableColumns -> %w", err1)
    }

    for _, row := range rows1 {
        //找列
        if util.InSlice(row[0], self.SkipColumns) {
            //跳过不需要核对的列
            skipcols = append(skipcols, row[0])
        } else {
            cols = append(cols, row[0])
        }

    }
    self.Columns = cols

    //获取主键
    if len(self.Keys) == 0 {
        rows2, err2 := self.DbGroup.SourceDbConn.QueryReturnList(sql2)
        if err2 != nil {
            return fmt.Errorf("getTableColumns -> %w", err1)
        }
        for _, row := range rows2 {
            pk = append(pk, row[0])
        }
        self.Keys = pk
    }

    slog.Infof("[%s.%s] 主键列: %s", self.DbName, self.TbName, strings.Join(self.Keys, ", "))
    if len(skipcols) > 0 {
        slog.Infof("[%s.%s] 跳过不需要核对的列: %s", self.DbName, self.TbName, strings.Join(skipcols, ", "))
    }
    return nil
}

func (self *Table) getCheckSql() error {
    //生成计算CRC32数据的SQL
    var sql string
    KeysText := strings.Join(util.EncloseStringArray(self.Keys, `"`), ", ")
    ColumnsText := strings.Join(util.EncloseStringArray(self.Columns, `"`), ", ")
    if self.Mode == "count" {
        self.SqlText = fmt.Sprintf("select count(*) cnt from %s", self.TbName)
    } else {
        if self.Mode == "slow" {
            sql = fmt.Sprintf("select concat_ws(',',%s) pk,concat_ws('|',%s) as rowdata from %s", KeysText, ColumnsText, self.TbName)
        } else {
            sql = fmt.Sprintf("select concat_ws(',',%s) pk,crc32(concat_ws('|',%s)) chksum from %s", KeysText, ColumnsText, self.TbName)
        }
        if self.Where != "" {
            sql += " where " + self.Where
        }
        self.SqlText = sql + " order by " + KeysText
    }
    slog.Infof("[%s.%s] SqlText: %s", self.DbName, self.TbName, self.SqlText)
    return nil
}

func (self *Table) PreCheck() bool {
    //预检查
    slog.Infof("[%s.%s] 执行预检查", self.DbName, self.TbName)

    //获取列名
    err := self.getTableColumns()
    if err != nil {
        self.Result.Status = -1
        self.Result.Message = err.Error()
        slog.Error(err)
        return false
    }

    err = self.getCheckSql()
    if err != nil {
        self.Result.Status = -1
        self.Result.Message = err.Error()
        slog.Error(err)
        return false
    }
    return true
}

func (self *Table) pullSourceDataSumFast(dataCh chan<- *model.Data, doneCh <-chan struct{}) error {
    //获取源端数据，在数据库上计算CRC32，性能高

    slog.Infof("[%s.%s] 开始下载source端数据", self.DbGroup.SourceDb, self.TbName)
    cur, err := self.DbGroup.SourceDbConn.ConnPool.Query(self.SqlText)
    if err != nil {
        panic(err)
    }
    defer cur.Close()

    for cur.Next() {
        data := model.Data{}
        err := cur.Scan(&data.Id, &data.Sum)
        if err != nil {
            panic(err)
        }
        select {
        case dataCh <- &data:
            self.Result.SourceRows++
        case <-doneCh:
            slog.Infof("收到停止信号，结束数据下载[%s.%s]", self.DbGroup.TargetDb, self.TbName)
            return nil
        }
    }

    return nil
}

func (self *Table) pullTargetDataSumFast(dataCh chan<- *model.Data, doneCh <-chan struct{}) error {
    //获取源端数据，在数据库侧计算CRC32，性能高

    cur, err := self.DbGroup.TargetDbConn.ConnPool.Query(self.SqlText)
    if err != nil {
        return fmt.Errorf("GetTargetCRC32Data:Query -> %w", err)
    }
    defer cur.Close() //当连接中断，这个操作会卡住60s+

    for cur.Next() {
        data := model.Data{}
        err := cur.Scan(&data.Id, &data.Sum)
        if err != nil {
            return fmt.Errorf("GetTargetCRC32Data:Scan -> %w", err)
        }
        select {
        case dataCh <- &data:
            self.Result.TargetRows++
        case <-doneCh:
            slog.Infof("收到停止信号，结束数据下载[%s.%s]", self.DbGroup.TargetDb, self.TbName)
            return nil
        }
    }

    return nil
}

func (self *Table) pullSourceDataSumSlow(dataCh chan<- *model.Data, doneCh <-chan struct{}) error {
    // 获取源端数据，在本地计算CRC32，速度慢

    var (
        id   string
        text string
    )
    cur, err := self.DbGroup.SourceDbConn.ConnPool.Query(self.SqlText)
    if err != nil {
        return fmt.Errorf("GetSourceCRC32DataSlow:Query -> %w", err)
    }
    defer cur.Close()

    for cur.Next() {
        err := cur.Scan(&id, &text)
        if err != nil {
            return fmt.Errorf("GetSourceCRC32DataSlow:Scan -> %w", err)
        }
        data := model.Data{Id: id, Sum: util.CRC32(&text)}
        select {
        case dataCh <- &data:
            self.Result.SourceRows++
        case <-doneCh:
            slog.Infof("收到停止信号，结束数据下载[%s.%s]", self.DbGroup.TargetDb, self.TbName)
            return nil
        }
    }
    return nil
}

func (self *Table) pullTargetDataSumSlow(dataCh chan<- *model.Data, doneCh <-chan struct{}) error {
    // 获取源端数据，在本地计算CRC32，速度慢

    var (
        id   string
        text string
    )
    cur, err := self.DbGroup.TargetDbConn.ConnPool.Query(self.SqlText)
    if err != nil {
        return fmt.Errorf("GetTargetCRC32DataSlow:Query -> %w", err)
    }
    defer cur.Close()

    for cur.Next() {
        err := cur.Scan(&id, &text)
        if err != nil {
            return fmt.Errorf("GetTargetCRC32DataSlow:Scan -> %w", err)
        }
        data := model.Data{Id: id, Sum: util.CRC32(&text)}
        select {
        case dataCh <- &data:
            self.Result.TargetRows++
        case <-doneCh:
            slog.Infof("收到停止信号，结束数据下载[%s.%s]", self.DbGroup.TargetDb, self.TbName)
            return nil
        }
    }
    return nil
}

func (self *Table) PullSourceDataSum(dataCh chan<- *model.Data, doneCh <-chan struct{}) {
    defer util.TimeCost()(fmt.Sprintf("[%s.%s] Source端数据下载完成", self.DbName, self.TbName))
    defer close(dataCh)

    slog.Infof("[%s.%s] 开始下载Source端数据", self.DbGroup.SourceDb, self.TbName)
    var err error
    if self.Mode == "slow" {
        err = self.pullSourceDataSumSlow(dataCh, doneCh)
    } else {
        err = self.pullSourceDataSumFast(dataCh, doneCh)
    }
    if err != nil {
        self.Result.Status = -1
        self.Result.Message = err.Error()
        slog.Error(self.Result.Message)
        return
    }
}

func (self *Table) PullTargetDataSum(dataCh chan<- *model.Data, doneCh <-chan struct{}) {
    defer util.TimeCost()(fmt.Sprintf("[%s.%s] Target端数据下载完成", self.DbName, self.TbName))
    defer close(dataCh)

    slog.Infof("[%s.%s] 开始下载Target端数据", self.DbGroup.TargetDb, self.TbName)
    var err error
    if self.Mode == "slow" {
        err = self.pullTargetDataSumSlow(dataCh, doneCh)
    } else {
        err = self.pullTargetDataSumFast(dataCh, doneCh)
    }
    if err != nil {
        self.Result.Status = -1
        self.Result.Message = err.Error()
        slog.Error(self.Result.Message)
        return
    }
}

func (self *Table) GetSourceTableCount() {
    // 返回表总行数
    defer util.TimeCost()(fmt.Sprintf("[%s.%s] Source端总行数统计完成", self.DbGroup.SourceDb, self.TbName))

    rows, err := self.DbGroup.SourceDbConn.QueryReturnList(self.SqlText)
    if err != nil {
        self.Result.Status = -1
        self.Result.Message = fmt.Errorf("GetSourceTableCount:QueryReturnList -> %w", err).Error()
        slog.Error(err)
        return
    }
    cnt, _ := strconv.Atoi(rows[0][0])
    self.Result.SourceRows = cnt
}

func (self *Table) GetTargetTableCount() {
    // 返回表总行数
    defer util.TimeCost()(fmt.Sprintf("[%s.%s] Target端总行数统计完成", self.DbGroup.TargetDb, self.TbName))

    rows, err := self.DbGroup.TargetDbConn.QueryReturnList(self.SqlText)
    if err != nil {
        self.Result.Status = -1
        self.Result.Message = fmt.Errorf("GetTargetTableCount:QueryReturnList -> %w", err).Error()
        slog.Error(err)
        return
    }
    cnt, _ := strconv.Atoi(rows[0][0])
    self.Result.TargetRows = cnt

}

func (self *Table) GetRepairSQL(idText string, mode int) (string, error) {
    // 生成修复数据的sql
    // mode:修复模式, -1:delete, 0:update  1:insert
    if !util.InSlice(mode, []int{-1, 0, 1}) {
        return "", fmt.Errorf("GetRepairSQL:Invalid mode %d", mode)
    }

    var where string
    ColumnsText := strings.Join(util.EncloseStringArray(self.Columns, `"`), ", ")

    //拼接where
    idValues := strings.Split(idText, ",")
    for i, v := range self.Keys {
        if i > 0 {
            where += fmt.Sprintf(` and "%s"='%s'`, v, idValues[i])
        } else {
            where += fmt.Sprintf(`"%s"='%s'`, v, idValues[i])
        }
    }

    var sqlText strings.Builder
    sql := fmt.Sprintf("select %s from %s where %s;\n", ColumnsText, self.TbName, where)
    switch mode {
    case -1:
        sqlText.WriteString(fmt.Sprintf("delete from %s where %s;\n", self.TbName, where))
    case 0:
        rows, err := self.DbGroup.SourceDbConn.QueryReturnList(sql)
        if err != nil {
            return "", err
        }
        //拼接update SQL
        for _, row := range rows {
            colVal := []string{}
            for i, col := range self.Columns {
                valText := strings.Replace(row[i], `'`, `''`, -1)
                colVal = append(colVal, fmt.Sprintf(`"%s"='%s'`, col, valText))
            }
            setText := strings.Join(colVal, ", ")
            sqlText.WriteString(fmt.Sprintf("UPDATE %s SET %s WHERE %s;\n", self.TbName, setText, where))
        }

    case 1:
        rows, err := self.DbGroup.SourceDbConn.QueryReturnText(sql)
        if err != nil {
            return "", err
        }
        for _, rowtext := range rows {
            sqlText.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;\n", self.TbName, ColumnsText, rowtext))
        }
    }
    return sqlText.String(), nil

}

func (self *Table) recheckOne(idText string) bool {
    //复核一行数据,相同返回true
    var sql string
    var where strings.Builder
    ColumnsText := strings.Join(util.EncloseStringArray(self.Columns, `"`), ", ")

    //拼接where
    idValues := strings.Split(idText, ",")
    for i, v := range self.Keys {
        if i > 0 {
            where.WriteString(fmt.Sprintf(` and "%s"='%s'`, v, idValues[i]))
        } else {
            where.WriteString(fmt.Sprintf(`"%s"='%s'`, v, idValues[i]))
        }
    }

    //核对数据
    sql = fmt.Sprintf("select %s from %s where %s", ColumnsText, self.TbName, where.String())
    srow, err := self.DbGroup.SourceDbConn.QueryReturnDict(sql)
    if err != nil {
        slog.Errorf("[%s.%s] 复核不一致的数据，查询Source端报错：%s", self.DbName, self.TbName, err)
        return false
    }
    trow, err := self.DbGroup.TargetDbConn.QueryReturnDict(sql)
    if err != nil {
        slog.Errorf("[%s.%s] 复核不一致的数据，查询Target端报错：%s", self.DbName, self.TbName, err)
        return false
    }
    if len(srow) == 0 && len(trow) == 0 {
        slog.Infof("[%s.%s] 两端均无此数据,复核通过 id:[%s]", self.DbName, self.TbName, idText)
        return true
    } else if len(srow) == 1 && len(srow) == len(trow) {
        if res, str := util.MapIsEqual(srow[0], trow[0]); res {
            slog.Infof("[%s.%s] 数据一致,复核通过 id:[%s]", self.DbName, self.TbName, idText)
            return true
        } else {
            slog.Infof("[%s.%s] 数据不一致,复核不通过 id:[%s] %s", self.DbName, self.TbName, idText, str)
        }
    } else {
        slog.Infof("[%s.%s] 两端数据行数不一致，复核不通过 id:[%s] rows:[%d] vs [%d]", self.DbName, self.TbName, idText, len(srow), len(trow))
    }
    return false
}

func (self *Table) Recheck(idTextList []string) (passList []string) {
    for _, idText := range idTextList {
        if self.recheckOne(idText) {
            passList = append(passList, idText)
        }
    }
    return passList
}
func (self *Table) GetResult() *model.Result {
    return self.Result
}
