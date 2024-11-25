package threading

import (
    "fmt"
    "github.com/gookit/slog"
    "os"
    "os/signal"
    "sync"
    "syscall"
)

var SignalChan chan os.Signal

// 接收kill 信号15
func init() {
    SignalChan = make(chan os.Signal)
    signal.Notify(SignalChan, syscall.SIGTERM)
}

type Pool struct {
    Queue chan func()
    Size  int
    Wg    *sync.WaitGroup
    Quit  chan struct{}
}

func NewPool(workerNum, queueSize int) *Pool {
    if workerNum <= 0 {
        panic("workerNum must > 0")
    }
    if queueSize <= 0 {
        panic("queueSize must > 0")
    }
    var wg sync.WaitGroup
    return &Pool{
        Queue: make(chan func(), queueSize),
        Size:  workerNum,
        Wg:    &wg,
        Quit:  make(chan struct{}, workerNum),
    }
}

func (p *Pool) Worker(num int) {
    defer p.Wg.Done()

    for {
        select {
        case task, ok := <-p.Queue:
            if !ok {
                slog.Infof(fmt.Sprintf("任务已完成，Worker%d退出", num))
                return
            }
            task()

        case <-p.Quit:
            slog.Infof(fmt.Sprintf("收到停止信号，Worker%d终止", num))
            return
        }
    }

}

func (p *Pool) Start() {
    slog.Infof("启动线程池，Size=%d", p.Size)
    for i := 1; i <= p.Size; i++ {
        p.Wg.Add(1)
        go p.Worker(i)
    }

    //接收kill信号，发送停止信号
    go func() {
        <-SignalChan
        slog.Infof("收到kill信号，准备退出程序")
        for i := 0; i < p.Size; i++ {
            p.Quit <- struct{}{}
        }
        //close(p.Quit)
    }()

}

func (p *Pool) Close() {
    defer slog.Infof("关闭任务队列，Size=%d", p.Size)
    close(p.Queue) //关闭任务队列
}

func (p *Pool) Join() {
    p.Wg.Wait()
}

func (p *Pool) AddTask(t func()) {
    p.Queue <- t
}
