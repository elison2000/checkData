package check

import (
    "checkData/model"
    "checkData/util"
    "fmt"
    "github.com/gookit/slog"
    "strings"
    "sync"
    "time"
)

type Table interface {
    GetDbName() string
    GetTbName() string
    PreCheck() bool
    PullSourceDataSum(chan<- *model.Data, <-chan struct{})
    PullTargetDataSum(chan<- *model.Data, <-chan struct{})
    Recheck([]string) []string
    GetRepairSQL(string, int) (string, error)
    GetSourceTableCount()
    GetTargetTableCount()
    GetResult() *model.Result
}

/*
   使用Checker注意事项:
   SourceDataChan和TargetDataChan 取到的数据必须是按id排序好的，也即是实现PushDataSumFromSource和PushDataSumFromTarget的方法时，需要保证数据是按id排序的，否则会导致数据核对结果出错
*/
type Checker struct {
    Table          Table
    Capacity       int
    SourceDataChan chan *model.Data
    TargetDataChan chan *model.Data
    SourceDoneChan chan struct{}
    TargetDoneChan chan struct{}
    SourceMore     map[string]uint32
    TargetMore     map[string]uint32
    Diff           []string
    Result         *model.Result
    Options        *model.Options
}

func NewChecker(t Table, opt *model.Options) *Checker {
    sData := make(chan *model.Data, opt.Capacity)
    tData := make(chan *model.Data, opt.Capacity)
    sDone := make(chan struct{}, 2)
    tDone := make(chan struct{}, 2)

    source := make(map[string]uint32, opt.Capacity)
    target := make(map[string]uint32, opt.Capacity)
    diff := make([]string, 0, opt.Capacity)
    return &Checker{
        Table:          t,
        Capacity:       opt.Capacity,
        SourceDataChan: sData,
        TargetDataChan: tData,
        SourceDoneChan: sDone,
        TargetDoneChan: tDone,
        SourceMore:     source,
        TargetMore:     target,
        Diff:           diff,
        Result:         t.GetResult(),
        Options:        opt,
    }
}

func (self *Checker) AddSame() {
    self.Result.SameRows++
}

func (self *Checker) AddDiff(key string) int {
    self.Diff = append(self.Diff, key)
    if len(self.Diff) >= self.Capacity {
        return -2
    }
    return 0
}

func (self *Checker) AddSourceMore(key string, val uint32) int {
    /* 返回值说明
       -2:容量已满  -1:未找到  0:值不一致  1:值一致
    */

    v, ok := self.TargetMore[key]
    if ok {
        delete(self.TargetMore, key)
        if val == v {
            self.AddSame()
            return 1
        } else {
            return self.AddDiff(key)
        }
    }

    //没找到
    self.SourceMore[key] = val
    if len(self.SourceMore) >= self.Capacity {
        return -2
    } else {
        return -1
    }
}

func (self *Checker) AddTargetMore(key string, val uint32) int {
    /* 返回值说明
       -2:容量已满  -1:未找到  0:值不一致  1:值一致
    */

    v, ok := self.SourceMore[key]
    if ok {
        delete(self.SourceMore, key)
        if val == v {
            self.AddSame()
            return 1
        } else {
            return self.AddDiff(key)
        }
    }

    //没找到
    self.TargetMore[key] = val
    if len(self.TargetMore) >= self.Capacity {
        return -2
    } else {
        return -1
    }

}

func (self *Checker) StopPull() {
    self.SourceDoneChan <- struct{}{}
    self.TargetDoneChan <- struct{}{}
}

func (self *Checker) CheckCount() {
    defer util.TimeCost()(fmt.Sprintf("[%s.%s] 表行数核对完成", self.Table.GetDbName(), self.Table.GetTbName()))
    slog.Infof("[%s.%s] 开始核对表行数", self.Table.GetDbName(), self.Table.GetTbName())
    var wg sync.WaitGroup
    wg.Add(2)
    go func() {
        defer wg.Done()
        self.Table.GetSourceTableCount()
    }()
    go func() {
        defer wg.Done()
        self.Table.GetTargetTableCount()
    }()

    //等待完成
    wg.Wait()

    if self.Result.Status == -1 {
        slog.Info(self.Result.GetShortLog())
        return
    }

    //对比行数
    if self.Result.SourceRows == self.Result.TargetRows {
        self.Result.Status = 1
        slog.Info(self.Result.GetShortLog())
    } else {
        self.Result.Status = 0
        slog.Info(self.Result.GetShortLog())
    }

}

func (self *Checker) CheckDetail() {
    // 核对明细

    defer util.TimeCost()(fmt.Sprintf("[%s.%s] 表明细数据核对完成", self.Table.GetDbName(), self.Table.GetTbName()))
    slog.Infof("[%s.%s] 开始核对表明细数据", self.Table.GetDbName(), self.Table.GetTbName())
    go self.Table.PullSourceDataSum(self.SourceDataChan, self.SourceDoneChan)
    go self.Table.PullTargetDataSum(self.TargetDataChan, self.TargetDoneChan)

    sdata, sok := <-self.SourceDataChan
    tdata, tok := <-self.TargetDataChan

    var ret int

    //双通道均有数据时
    for sok && tok {

        if sdata.Id == tdata.Id {
            // 主键一样
            if sdata.Sum == tdata.Sum {
                self.AddSame()
            } else {
                ret = self.AddDiff(sdata.Id)
                if ret == -2 {
                    self.StopPull()
                    return
                }
            }
            sdata, sok = <-self.SourceDataChan
            tdata, tok = <-self.TargetDataChan
            continue
        }

        // 取出的id不一样时，使用checker核对
        ret = self.AddSourceMore(sdata.Id, sdata.Sum)
        switch ret {
        case 0, 1:
            sdata, sok = <-self.SourceDataChan
            continue
        case -2:
            self.StopPull()
            return
        }

    label:
        ret = self.AddTargetMore(tdata.Id, tdata.Sum)
        switch ret {
        case 0, 1:
            tdata, tok = <-self.TargetDataChan
            //在上一个步骤chk.AddSourceMore中，sdata已存入SourceMore，下一个数据不能再和sdata对比，直接AddTargetMore
            if tok {
                goto label
            }
            continue
        case -2:
            self.StopPull()
            return
        }

        //都找不到时，重新取值对比
        sdata, sok = <-self.SourceDataChan
        tdata, tok = <-self.TargetDataChan

    }

    if sok {
        ret = self.AddSourceMore(sdata.Id, sdata.Sum)
        for sdata = range self.SourceDataChan {
            ret = self.AddSourceMore(sdata.Id, sdata.Sum)
            if ret == -2 {
                self.StopPull()
                return
            }
        }
    }

    if tok {
        ret = self.AddTargetMore(tdata.Id, tdata.Sum)
        for tdata = range self.TargetDataChan {
            ret = self.AddTargetMore(tdata.Id, tdata.Sum)
            if ret == -2 {
                self.StopPull()
                return
            }
        }
    }

}

func (self *Checker) Recheck() {

    //复核
    if self.Result.Status == -1 {
        slog.Infof("[%s.%s] 初核失败，跳过复核", self.Table.GetDbName(), self.Table.GetTbName())
        return
    }

    toRecheckRows := len(self.Diff) + len(self.SourceMore) + len(self.TargetMore)
    if toRecheckRows == 0 {
        slog.Infof("[%s.%s] 初核通过，跳过复核", self.Table.GetDbName(), self.Table.GetTbName())
        return
    }

    if toRecheckRows > self.Options.MaxRecheckRows {
        slog.Infof("[%s.%s] 不一致行数:%d，大于--max-recheck-rows参数，跳过复核", self.Table.GetDbName(), self.Table.GetTbName(), toRecheckRows)
        return
    }

    if self.Options.MaxRecheckTimes <= 0 {
        slog.Infof("[%s.%s] 最大复核次数为0，跳过复核", self.Table.GetDbName(), self.Table.GetTbName())
        return
    }

    if self.Options.MaxRecheckRows <= 0 {
        slog.Infof("[%s.%s] 最大复核行数为0，跳过复核", self.Table.GetDbName(), self.Table.GetTbName())
        return
    }

    if len(self.Diff) > self.Options.Capacity || len(self.SourceMore) > self.Options.Capacity || len(self.TargetMore) > self.Options.Capacity {
        self.Result.Status = 0
        self.Result.Message = fmt.Sprintf("存放不一致数据的队列已满，跳过复核")
    }

    defer util.TimeCost()(fmt.Sprintf("[%s.%s] 表明细数据复核完成", self.Table.GetDbName(), self.Table.GetTbName()))
    slog.Infof("[%s.%s] 开始复核表明细数据", self.Table.GetDbName(), self.Table.GetTbName())
    idTextList := make([]string, 0)
    recheckPassList := make([]string, 0)
    for _, idText := range self.Diff {
        idTextList = append(idTextList, idText)
    }

    for idText, _ := range self.SourceMore {
        idTextList = append(idTextList, idText)
    }

    for idText, _ := range self.TargetMore {
        idTextList = append(idTextList, idText)
    }

    for i := 1; i <= self.Options.MaxRecheckTimes; i++ {
        if len(idTextList) == 0 {
            break
        }

        if i > 1 {
            time.Sleep(time.Second * 10)
        }

        slog.Infof("[%s.%s] 第 %d 次复核开始", self.Table.GetDbName(), self.Table.GetTbName(), i)
        passList := self.Table.Recheck(idTextList)
        if len(passList) == 0 {
            util.RemoveSliceMultiElement(&idTextList, &passList) //剔除复核通过的记录
            recheckPassList = append(recheckPassList, passList...)
        }

        slog.Infof("[%s.%s] 第 %d 次复核结束", self.Table.GetDbName(), self.Table.GetTbName(), i)
        slog.Infof("[%s.%s] 复核总行数:%d 复核通过总行数:%d 本次复核通过行数:%d", self.Table.GetDbName(), self.Table.GetTbName(), toRecheckRows, len(recheckPassList), len(passList))

    }

    for _, v := range recheckPassList {
        delete(self.SourceMore, v)
        delete(self.TargetMore, v)
    }

    //剔除复核通过的记录
    util.RemoveSliceMultiElement(&self.Diff, &recheckPassList)

    self.Result.RecheckPassRows = len(recheckPassList)

}

func (self *Checker) Start() {
    defer util.TimeCost()(fmt.Sprintf("[%s.%s] 核对完成", self.Table.GetDbName(), self.Table.GetTbName()))
    slog.Infof("[%s.%s] 开始核对", self.Table.GetDbName(), self.Table.GetTbName())
    t := time.Now()

    if !self.Table.PreCheck() {
        slog.Errorf("[%s.%s] 预检查不通过", self.Table.GetDbName(), self.Table.GetTbName())
        return
    }

    if self.Options.Mode == "count" {
        self.CheckCount()
    } else {
        self.CheckDetail()
        self.Recheck()
    }

    self.Result.ExecuteSeconds = int(time.Since(t).Seconds())

    if self.Options.Mode != "count" {
        //计算最终结果
        self.settle()

        //导出修复SQL
        if self.Result.RecheckPassRows != -1 {
            self.SaveRepairSQL()
        }
    }

}

func (self *Checker) settle() {
    self.Result.DiffRows = len(self.Diff)
    self.Result.SourceMoreRows = len(self.SourceMore)
    self.Result.TargetMoreRows = len(self.TargetMore)

    if self.Result.Status == -1 {
        return
    }

    if self.Result.DiffRows+self.Result.SourceMoreRows+self.Result.TargetMoreRows > 0 {
        self.Result.Status = 0
        return
    }

    self.Result.Status = 1

    if self.Result.RecheckPassRows > 0 {
        self.Result.Message = "复核通过"
    }
    return
}

func (self *Checker) SaveRepairSQL() {
    //defer util.TimeCost()(fmt.Sprintf("[%s.%s] 保存修复SQL完成", self.Table.GetDbName(), self.Table.GetTbName()))

    var sqlText strings.Builder
    if len(self.TargetMore) > 0 {
        sqlText.Reset()
        for idText, _ := range self.TargetMore {
            _sql, err := self.Table.GetRepairSQL(idText, -1)
            if err != nil {
                slog.Errorf("[%s.%s] 导出delete.sql文件报错: %s", self.Table.GetDbName(), self.Table.GetTbName(), err)
            } else {
                sqlText.WriteString(_sql)
            }
        }
        deleteFile := fmt.Sprintf("%s/%s/%s.delete.sql", self.Options.BaseDir, self.Table.GetDbName(), self.Table.GetTbName())
        util.WriteFile(deleteFile, sqlText.String())
    }

    if len(self.SourceMore) > 0 {
        sqlText.Reset()
        for idText, _ := range self.SourceMore {
            _sql, err := self.Table.GetRepairSQL(idText, 1)
            if err != nil {
                slog.Errorf("[%s.%s] 导出insert.sql文件报错: %s", self.Table.GetDbName(), self.Table.GetTbName(), err)
            } else {
                sqlText.WriteString(_sql)
            }
        }
        insertFile := fmt.Sprintf("%s/%s/%s.insert.sql", self.Options.BaseDir, self.Table.GetDbName(), self.Table.GetTbName())
        util.WriteFile(insertFile, sqlText.String())
    }

    if len(self.Diff) > 0 {
        sqlText.Reset()
        for _, idText := range self.Diff {
            _sql, err := self.Table.GetRepairSQL(idText, 0)
            if err != nil {
                slog.Errorf("[%s.%s] 导出update.sql文件报错: %s", self.Table.GetDbName(), self.Table.GetTbName(), err)
            } else {
                sqlText.WriteString(_sql)
            }
        }
        updateFile := fmt.Sprintf("%s/%s/%s.update.sql", self.Options.BaseDir, self.Table.GetDbName(), self.Table.GetTbName())
        util.WriteFile(updateFile, sqlText.String())
    }

}
func (self *Checker) SaveResult() {
    csvFileName := fmt.Sprintf("%s/%s.csv", self.Options.BaseDir, self.Table.GetDbName())

    var status string
    switch self.Result.Status {
    case 0:
        status = "不一致"
    case 1:
        status = "一致"
    default:
        status = "未知"
    }

    text := fmt.Sprintf("%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%s\n", self.Result.DbName, self.Result.TbName, status, self.Result.ExecuteSeconds,
        self.Result.SourceRows, self.Result.TargetRows, self.Result.SameRows, self.Result.DiffRows, self.Result.SourceMoreRows, self.Result.TargetMoreRows, self.Result.RecheckPassRows, self.Result.Message)
    util.WriteFileTail(csvFileName, text)

    if self.Options.Mode == "count" {
        slog.Info(fmt.Sprintf("[%s.%s] 核对结果 [Status:%d SourceRows:%d TargetRows:%d]",
            self.Result.DbName, self.Result.TbName, self.Result.Status, self.Result.SourceRows, self.Result.TargetRows))
    } else {
        slog.Info(fmt.Sprintf("[%s.%s] 核对结果 [Status:%d SourceRows:%d TargetRows:%d SameRows:%d DiffRows:%d SourceMoreRows:%d TargetMoreRows:%d RecheckPassRows:%d]",
            self.Result.DbName, self.Result.TbName, self.Result.Status, self.Result.SourceRows, self.Result.TargetRows, self.Result.SameRows, self.Result.DiffRows, self.Result.SourceMoreRows, self.Result.TargetMoreRows, self.Result.RecheckPassRows))
    }

    if len(self.Diff) > 0 {
        diffFileName := fmt.Sprintf("%s/%s/%s.diff", self.Options.BaseDir, self.Table.GetDbName(), self.Table.GetTbName())
        diffFile, err := util.File(diffFileName)
        if err != nil {
            slog.Errorf("写入文件%s报错: %s", diffFileName, err)
        } else {
            for _, id := range self.Diff {
                diffFile.WriteString(id + "\n")
            }
            diffFile.Close()
        }
    }

    if len(self.SourceMore) > 0 {
        tLossFileName := fmt.Sprintf("%s/%s/%s.tlost", self.Options.BaseDir, self.Table.GetDbName(), self.Table.GetTbName())
        tLossFile, err := util.File(tLossFileName)
        if err != nil {
            slog.Errorf("写入文件%s报错: %s", tLossFileName, err)
        } else {
            for id, _ := range self.SourceMore {
                tLossFile.WriteString(id + "\n")
            }
            tLossFile.Close()
        }
    }

    if len(self.TargetMore) > 0 {
        tMoreFileName := fmt.Sprintf("%s/%s/%s.tmore", self.Options.BaseDir, self.Table.GetDbName(), self.Table.GetTbName())
        tMoreFile, err := util.File(tMoreFileName)
        if err != nil {
            slog.Errorf("写入文件%s报错: %s", tMoreFileName, err)
        } else {
            for id, _ := range self.TargetMore {
                tMoreFile.WriteString(id + "\n")
            }
            tMoreFile.Close()
        }
    }
}
