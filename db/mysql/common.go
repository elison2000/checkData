package mysql

import (
	"checkData/model"
	"checkData/util"
	"database/sql"
	"fmt"
	"github.com/gookit/slog"
	"strconv"
	"strings"
)

func (self *Table) PreCheck() bool {
	//预检查
	defer func() { slog.Infof("[%s.%s] SQLText: %s", self.DbName, self.TbName, self.SQLText) }()

	slog.Infof("[%s.%s] 执行预检查", self.DbName, self.TbName)

	self.getEnclosedTbName()

	if self.Mode == "count" {
		self.SQLText = fmt.Sprintf("select count(*) cnt from %s", self.EnclosedTbName)
		if self.Where != "" {
			self.SQLText += " where " + self.Where
		}
		return true
	}

	//获取主键
	err := self.getKeys()
	if err != nil {
		self.Result.Status = -1
		self.Result.Message = err.Error()
		slog.Error(err)
		return false
	}

	//获取列名
	err = self.getColumns()
	if err != nil {
		self.Result.Status = -1
		self.Result.Message = err.Error()
		slog.Error(err)
		return false
	}

	//提除主键列和跳过的列
	var _tmp []string
	var skipCols []string
	for _, v := range self.Columns {
		if util.InSlice(v, self.Keys) {
			continue
		} else if util.InSlice(v, self.SkipColumns) {
			skipCols = append(skipCols, v)
		} else {
			_tmp = append(_tmp, v)
		}
	}
	self.Columns = _tmp

	if len(skipCols) > 0 {
		slog.Infof("[%s.%s] 跳过不需要核对的列: %s", self.DbName, self.TbName, strings.Join(skipCols, ", "))
	}

	if len(self.Keys) == 0 {
		self.Result.Status = -1
		self.Result.Message = "Keys is empty"
		slog.Error(fmt.Errorf("Keys is empty"))
		return false
	}

	if len(self.Columns) == 0 {
		self.Result.Status = -1
		self.Result.Message = "Columns is empty"
		slog.Error(fmt.Errorf("Columns is empty"))
		return false
	}

	self.KeysText = util.EncloseAndJoin(self.Keys, quote)
	self.ColumnsText = util.EncloseAndJoin(self.Columns, quote)

	err = self.getCheckSQL()
	if err != nil {
		self.Result.Status = -1
		self.Result.Message = err.Error()
		slog.Error(err)
		return false
	}
	return true
}

func (self *Table) pullSourceDataSumFast(dataCh chan<- *model.Data, doneCh <-chan struct{}) error {
	//获取源端数据，在数据库侧计算CRC32，性能高

	cur, err := self.DbGroup.SourceDbConn.Query(self.SQLText)
	if err != nil {
		return fmt.Errorf("GetSourceCRC32Data:Query -> %w", err)
	}
	defer cur.Close() //当连接中断，这个操作会卡住60s+

	for cur.Next() {
		data := model.Data{}
		err := cur.Scan(&data.Id, &data.Sum)
		if err != nil {
			return fmt.Errorf("GetSourceCRC32Data:Scan -> %w", err)
		}
		select {
		case dataCh <- &data:
			self.Result.SourceRows++
		case <-doneCh:
			slog.Infof("收到停止信号，结束数据下载[%s.%s]", self.DbGroup.SourceDb, self.TbName)
			return nil
		}
	}

	return nil
}

func (self *Table) pullTargetDataSumFast(dataCh chan<- *model.Data, doneCh <-chan struct{}) error {
	//获取源端数据，在数据库侧计算CRC32，性能高
	cur, err := self.DbGroup.TargetDbConn.Query(self.SQLText)
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

	cur, err := self.DbGroup.SourceDbConn.Query(self.SQLText)
	if err != nil {
		return fmt.Errorf("GetSourceCRC32DataSlow:Query-> %w", err)
	}
	defer cur.Close()

	columns, err := cur.Columns()
	if err != nil {
		return err
	}

	values := make([]*sql.RawBytes, len(columns))
	valuesP := make([]interface{}, len(columns))
	for i := range values {
		valuesP[i] = &values[i]
	}

	var buf1 strings.Builder
	var buf2 []byte
	var sum uint32

	for cur.Next() {

		if err := cur.Scan(valuesP...); err != nil {
			return err
		}

		buf1.Reset()
		buf2 = []byte{}

		//拼接id
		for i := 0; i < len(self.Keys); i++ {
			if i > 0 {
				buf1.WriteString(",")
			}

			if values[i] == nil {
				buf1.WriteString("NULL")
			} else {
				buf1.Write(*values[i])
			}
		}

		// 拼接数据
		for i := len(self.Keys); i < len(values); i++ {
			if values[i] == nil {
				buf2 = append(buf2, []byte("NULL")...)
			} else {
				buf2 = append(buf2, *values[i]...)
			}

		}

		sum = util.CRC32Bytes(buf2)
		data := model.Data{Id: buf1.String(), Sum: sum}
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

	cur, err := self.DbGroup.TargetDbConn.Query(self.SQLText)
	if err != nil {
		return fmt.Errorf("GetTargetCRC32DataSlow:Query-> %w", err)
	}
	defer cur.Close()

	columns, err := cur.Columns()
	if err != nil {
		return err
	}

	values := make([]*sql.RawBytes, len(columns))
	valuesP := make([]interface{}, len(columns))
	for i := range values {
		valuesP[i] = &values[i]
	}

	var buf1 strings.Builder
	var buf2 []byte

	for cur.Next() {

		if err := cur.Scan(valuesP...); err != nil {
			return err
		}

		buf1.Reset()
		buf2 = []byte{}

		//拼接id
		for i := 0; i < len(self.Keys); i++ {
			if i > 0 {
				buf1.WriteString(",")
			}

			if values[i] == nil {
				buf1.WriteString("NULL")
			} else {
				buf1.Write(*values[i])
			}
		}

		// 拼接数据
		for i := len(self.Keys); i < len(values); i++ {
			if values[i] == nil {
				buf2 = append(buf2, []byte("NULL")...)
			} else {
				buf2 = append(buf2, *values[i]...)
			}
		}

		data := model.Data{Id: buf1.String(), Sum: util.CRC32Bytes(buf2)}
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

	rows, err := util.QueryReturnList(self.DbGroup.SourceDbConn, self.SQLText)
	if err != nil {
		self.Result.Status = -1
		self.Result.Message = fmt.Errorf("GetSourceTableCount -> %w", err).Error()
		slog.Error(err)
		return
	}
	cnt, err := strconv.Atoi(rows[0][0])
	if err != nil {
		self.Result.Status = -1
		self.Result.Message = fmt.Errorf("GetSourceTableCount:Atoi -> %w", err).Error()
		slog.Error(err)
		return
	}
	self.Result.SourceRows = cnt
}

func (self *Table) GetTargetTableCount() {
	// 返回表总行数
	defer util.TimeCost()(fmt.Sprintf("[%s.%s] Target端总行数统计完成", self.DbGroup.TargetDb, self.TbName))

	rows, err := util.QueryReturnList(self.DbGroup.TargetDbConn, self.SQLText)
	if err != nil {
		self.Result.Status = -1
		self.Result.Message = fmt.Errorf("GetTargetTableCount -> %w", err).Error()
		slog.Error(err)
		return
	}
	cnt, err := strconv.Atoi(rows[0][0])
	if err != nil {
		self.Result.Status = -1
		self.Result.Message = fmt.Errorf("GetSourceTableCount:Atoi -> %w", err).Error()
		slog.Error(err)
		return
	}
	self.Result.TargetRows = cnt

}

func (self *Table) getWhereClause(idText string) (string, error) {
	//预处理主键列值
	_ids := strings.Split(idText, ",")
	var ids []string
	for i := range _ids {
		ids = append(ids, util.EncloseStr(_ids[i], "'"))
	}

	//拼接where
	whereClause, err := util.GenerateClause(self.Keys, ids, quote, "AND")
	if err != nil {
		return "", fmt.Errorf("getWhereClause -> %w", err)
	}
	return whereClause, nil
}

func (self *Table) recheckOne(idText string) bool {
	//复核一行数据,相同返回true
	whereClause, err := self.getWhereClause(idText)
	if err != nil {
		slog.Errorf("[%s.%s] 复核失败，生成whereClause报错：%s", self.DbName, self.TbName, err)
		return false
	}

	//核对数据
	sql := fmt.Sprintf("select %s from %s where %s", self.ColumnsText, self.EnclosedTbName, whereClause)
	srow, err := util.QueryReturnDict(self.DbGroup.SourceDbConn, sql)
	if err != nil {
		slog.Errorf("[%s.%s] 复核不一致的数据，查询Source端报错：%s", self.DbName, self.TbName, err)
		return false
	}
	trow, err := util.QueryReturnDict(self.DbGroup.TargetDbConn, sql)
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

func (self *Table) GetRepairSQL(idText string, mode int) (string, error) {
	// 生成修复数据的sql
	// mode:修复模式, -1:delete, 0:update  1:insert
	if !util.InSlice(mode, []int{-1, 0, 1}) {
		return "", fmt.Errorf("GetRepairSQL:Invalid mode %d", mode)
	}

	var columns []string
	columns = append(columns, self.Keys...)
	columns = append(columns, self.Columns...)
	columnsText := util.EncloseAndJoin(columns, quote)

	whereClause, err := self.getWhereClause(idText)
	if err != nil {
		return "", fmt.Errorf("GetRepairSQL -> %w", err)
	}

	var sqlText strings.Builder
	switch mode {
	case -1:
		//生成delete SQL
		sqlText.WriteString(fmt.Sprintf("DELETE FROM %s WHERE %s;\n", self.EnclosedTbName, whereClause))
	case 0:
		//生成update SQL
		sql := fmt.Sprintf("select %s from %s where %s", util.EncloseAndJoin(self.Columns, quote), self.EnclosedTbName, whereClause)
		rows, err := util.QueryReturnListWithNil(self.DbGroup.SourceDbConn, sql)
		if err != nil {
			return "", err
		}
		row := util.EncloseValues(rows[0], self.escapeValue)
		var setClause string
		setClause, err = util.GenerateClause(self.Columns, row, quote, ",")
		if err != nil {
			return "", fmt.Errorf("GetRepairSQL:GenerateClause -> %w", err)
		}

		sqlText.WriteString(fmt.Sprintf("UPDATE %s SET %s WHERE %s;\n", self.EnclosedTbName, setClause, whereClause))

	case 1:
		//生成insert SQL
		sql := fmt.Sprintf("select %s from %s where %s", columnsText, self.EnclosedTbName, whereClause)
		rows, err := util.QueryReturnListWithNil(self.DbGroup.SourceDbConn, sql)
		if err != nil {
			return "", err
		}

		for _, v := range rows {
			row := util.EncloseValues(v, self.escapeValue)
			sqlText.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);\n", self.EnclosedTbName, columnsText, strings.Join(row, ", ")))
		}
	}

	return sqlText.String(), nil
}

func (self *Table) GetResult() *model.Result {
	return self.Result
}
