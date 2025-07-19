package model

import "fmt"

type Data struct {
	Id  string
	Sum uint32
}

type TableInfo struct {
	Source     []string
	Target     []string
	Skip       []string
	ToCheck    []string
	SourceMore []string
	TargetMore []string
}

type Result struct {
	DbName          string
	TbName          string
	Status          int //-1:未知,0:不一致,1:一致
	Message         string
	SourceRows      int
	TargetRows      int
	SameRows        int
	DiffRows        int
	SourceMoreRows  int
	TargetMoreRows  int
	RecheckPassRows int
	ExecuteSeconds  int
}

func (self *Result) GetLog() string {
	return fmt.Sprintf("[%s.%s] [Status:%d SourceRows:%d TargetRows:%d SameRows:%d DiffRows:%d SourceMoreRows:%d TargetMoreRows:%d RecheckPassRows:%d]", self.DbName, self.TbName, self.Status, self.SourceRows, self.TargetRows, self.SameRows, self.DiffRows, self.SourceMoreRows, self.TargetMoreRows, self.RecheckPassRows)
}

func (self *Result) GetShortLog() string {
	return fmt.Sprintf("[%s.%s] [Mode:count Status:%d SourceRows:%d TargetRows:%d]", self.DbName, self.TbName, self.Status, self.SourceRows, self.TargetRows)
}
