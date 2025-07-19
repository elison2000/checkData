package model

type Database interface {
	PreCheck() (err error)
	GetTableInfo() *TableInfo
	NewTable(tb string) Table
	Close()
}

type Table interface {
	GetDbName() string
	GetTbName() string
	PreCheck() bool
	PullSourceDataSum(chan<- *Data, <-chan struct{})
	PullTargetDataSum(chan<- *Data, <-chan struct{})
	Recheck([]string) []string
	GetRepairSQL(string, int) (string, error)
	GetSourceTableCount()
	GetTargetTableCount()
	GetResult() *Result
}
