package pool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	NoTimeOut        = -1 * time.Nanosecond
	NoLimitQueueSize = math.MaxInt32
)

type Pool struct {
	maxWorkerNum   int
	minWorkerNum   int
	waitQueueSize  int
	fair           bool
	name           string
	maxIdleTime    time.Duration
	workers        map[int32]*worker
	lock           sync.Mutex
	waitChan       chan *Task
	syncChan       chan *Task
	futureChan     chan *Future
	iterChan       chan *Future
	future         *Future
	last           unsafe.Pointer
	waitResult     int32
	lastCreateTime time.Time
	ctx            context.Context
	cancel         context.CancelFunc
	syncFutureDone bool
}

type Option func(p *Pool)

type Task struct {
	Id         string
	Run        func() (interface{}, error)
	saveResult bool
}

type Future struct {
	Id    string
	res   interface{}
	error error
	next  *Future
}

func (f *Future) Get() (interface{}, error) {
	return f.res, f.error
}

func NewCachedPool(options ...Option) (*Pool, error) {
	o := []Option{MaxIdleTime(time.Minute)}
	if len(options) > 0 {
		o = append(o, options...)
	}
	o = append(o, WaitQueueSize(1))
	return NewPool(0, math.MaxInt32, o...)
}

func NewFixedPool(workerNum int, options ...Option) (*Pool, error) {
	return NewPool(workerNum, workerNum, options...)
}

//default math.MaxInt32,if size<1 set to default
func WaitQueueSize(size int) Option {
	return func(p *Pool) {
		if size > 0 {
			p.waitQueueSize = size
		}
	}
}

//default false
func Fair(fair bool) Option {
	return func(p *Pool) {
		p.fair = fair
	}
}

//default no timeout
func MaxIdleTime(timeout time.Duration) Option {
	return func(p *Pool) {
		if timeout > NoTimeOut {
			p.maxIdleTime = timeout
		}
	}
}

//worker pool name,default was unix timestamp
func Name(name string) Option {
	return func(p *Pool) {
		if len(name) > 0 {
			p.name = name
		}
	}
}

func NewPool(minWorkerNum, maxWorkerNum int, options ...Option) (*Pool, error) {
	if minWorkerNum < 0 || maxWorkerNum < 0 || minWorkerNum > maxWorkerNum {
		return nil, errors.New("invalid args")
	}
	ctx, cancel := context.WithCancel(context.Background())
	pool := &Pool{
		maxWorkerNum:   maxWorkerNum,
		minWorkerNum:   minWorkerNum,
		maxIdleTime:    NoTimeOut,
		waitQueueSize:  NoLimitQueueSize,
		fair:           false,
		name:           fmt.Sprintf("%d", time.Now().Unix()),
		syncChan:       make(chan *Task, 1),
		workers:        map[int32]*worker{},
		futureChan:     make(chan *Future, maxWorkerNum),
		ctx:            ctx,
		cancel:         cancel,
		syncFutureDone: false,
	}

	for _, option := range options {
		option(pool)
	}
	pool.waitChan = make(chan *Task, pool.waitQueueSize)
	iterChanSize := 1
	if pool.waitQueueSize > iterChanSize {
		iterChanSize = pool.waitQueueSize / 2
	}
	pool.iterChan = make(chan *Future, iterChanSize)
	go pool.sync()
	go pool.syncFuture()
	go pool.syncIter()
	return pool, nil
}

//---------------pool--------------------
//only execute task
func (p *Pool) Execute(task *Task) error {
	return p.sendTask(task, false)
}

//execute task and return result
func (p *Pool) Submit(task *Task) error {
	return p.sendTask(task, true)
}

//max wait time when waitResult is zero,must larger than maxIdleTime and zero.
func (p *Pool) Poll(maxWaitTime time.Duration) <-chan *Future {
	if maxWaitTime <= p.maxIdleTime || maxWaitTime <= 0 {
		panic(errors.New("MaxWaitTime must larger than maxIdleTime and zero"))
	}
	var (
		c = make(chan *Future)
	)
	go func() {
		var timer <-chan time.Time
		defer func() {
			close(c)
		}()
		for {
			select {
			case f, OK := <-p.iterChan:
				if OK {
					c <- f
				} else {
					log.Printf("Quit poll method (worker pool[%s] was shutdown).", p.name)
					return
				}
			case <-*timeout(&timer, maxWaitTime, func() bool {
				return atomic.LoadInt32(&p.waitResult) == 0
			}):
				log.Printf("Quit poll method (after waitResult is zero, maximum waiting time [%s] was reached). ", maxWaitTime)
				return
			}
		}
	}()
	return c
}

//shutdown the worker pool and wait for return the result of the running task
func (p *Pool) ShutDown() {
	p.cancel()
	log.Printf("worker pool [%s] shutdown.", p.name)
}

//shutdown the worker pool immediately
func (p *Pool) ShutDownNow() {
	p.ctx = context.WithValue(p.ctx, "force", true)
	p.cancel()
	log.Printf("worker pool [%s] shutdown now.", p.name)
}

//Return true if ShutDown or ShutDownNow was called, else false
func (p *Pool) IsShutDown() bool {
	return p.ctx.Err() != nil
}

func (p *Pool) sendTask(task *Task, saveResult bool) error {
	if p.ctx.Err() != nil {
		return errors.New(fmt.Sprintf("Workpool[%s] has been shutdown. ", p.name))
	}
	if len(p.workers) < p.minWorkerNum {
		p.newWorker()
	}
	if saveResult {
		task.saveResult = true
		atomic.AddInt32(&p.waitResult, 1)
	}
	select {
	case <-p.ctx.Done():
	case p.syncChan <- task:
	}
	return nil
}

func (p *Pool) sync() {
	defer func() {
		close(p.syncChan)
		close(p.waitChan)
	}()
	for {
		select {
		case task := <-p.syncChan:
			for {
				select {
				case p.waitChan <- task:
					break
				case <-p.ctx.Done():
					return
				default:
					if len(p.workers) < p.maxWorkerNum {
						p.newWorker()
						time.Sleep(100 * time.Millisecond)
					}
					continue
				}
				break
			}
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Pool) syncFuture() {
	defer func() {
		close(p.futureChan)
		p.syncFutureDone = true
	}()
	for {
		select {
		case f := <-p.futureChan:
			for {
				lp := atomic.LoadPointer(&p.last)
				if atomic.CompareAndSwapPointer(&p.last, lp, unsafe.Pointer(f)) {
					if lp == nil {
						p.future = (*Future)(p.last)
					} else {
						((*Future)(lp)).next = f
					}
					break
				}
			}
		default:
			if p.ctx.Err() != nil && len(p.workers) == 0 {
				return
			}
		}
	}
}

func (p *Pool) syncIter() {
	defer func() {
		close(p.iterChan)
		p.future = nil
	}()
	var last *Future
	for {
		if p.future == nil {
			if p.syncFutureDone {
				break
			}
			continue
		} else if last == p.future {
			if p.future.next == nil {
				if p.syncFutureDone {
					break
				}
				continue
			} else {
				p.future = p.future.next
			}
		}
		select {
		case p.iterChan <- p.future:
			atomic.AddInt32(&p.waitResult, -1)
			last = p.future
			if p.future.next != nil {
				p.future = p.future.next
			}
		}
	}
}

func (p *Pool) newWorker() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if len(p.workers) < p.minWorkerNum || len(p.workers) < p.maxWorkerNum {
		sn := p.getSeqNum()
		w := &worker{Id: sn, p: p, c: make(chan *Future, 1)}
		p.workers[sn] = w
		p.lastCreateTime = time.Now()
		go w.start()
		log.Printf("Start new worker[%d].", sn)
	}
}

func (p *Pool) destroyWorker(workerId int32, force bool) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	deleted := false
	if !force && time.Now().Before(p.lastCreateTime.Add(p.maxIdleTime)) {
		return false
	}
	if len(p.workers) > p.minWorkerNum || force {
		deleted = true
		p.workers[workerId].p = nil
		delete(p.workers, workerId)
		if force {
			log.Printf("Worker[%d] exits (worker pool[%s] was shutdown).", workerId, p.name)
		} else {
			log.Printf("Worker[%d] exits (max idle time[%v] was reached).", workerId, p.maxIdleTime)
		}
	}

	return deleted
}

func (p *Pool) getTask() *Task {
	var (
		task  *Task
		timer <-chan time.Time
	)
	if p.fair {
		for {
			select {
			case task = <-p.waitChan:
			case <-*timeout(&timer, p.maxIdleTime, func() bool {
				return p.maxIdleTime > NoTimeOut && timer == nil
			}):
			}
			return task
		}
	} else {
		for {
			select {
			case task = <-p.syncChan:
			case task = <-p.waitChan:
			case <-*timeout(&timer, p.maxIdleTime, func() bool {
				return p.maxIdleTime > NoTimeOut && timer == nil
			}):

			}
			return task
		}
	}
}

func (p *Pool) getSeqNum() int32 {
	return atomic.AddInt32(&seqNum, 1)
}

//---------------pool--------------------

//-------worker-----------
type worker struct {
	Id int32
	p  *Pool
	c  chan *Future
}

func (w *worker) start() {
	defer func() {
		if err := recover(); err != nil {
		}
		w.p.destroyWorker(w.Id, true)
	}()

	for w.p.ctx.Err() == nil {
		task := w.p.getTask()
		if task == nil {
			if w.p.destroyWorker(w.Id, false) {
				break
			}
			continue
		}
		go func(c chan *Future) {
			time.Sleep(5 * time.Second)
			result, err := task.Run()
			if task.saveResult {
				c <- &Future{Id: task.Id, res: result, error: err}
			} else {
				c <- nil
			}
		}(w.c)
		for {
			select {
			case <-w.p.ctx.Done():
				if w.p.ctx.Value("force") == true {
					panic(nil)
				} else {
					continue
				}
			case f := <-w.c:
				if f != nil && task.saveResult {
					if w.p.ctx.Err() == nil {
						go addFuture(w.p.ctx, w.p.futureChan, f)
					} else {
						addFuture(w.p.ctx, w.p.futureChan, f)
					}
				}
			}
			break
		}
	}
}

//-------worker-----------

var (
	seqNum = int32(0)
)

func addFuture(ctx context.Context, futureChan chan *Future, future *Future) {
	for {
		select {
		case futureChan <- future:
			return
		case <-ctx.Done():
			if ctx.Value("force") == true {
				return
			} else {
				continue
			}
		}
	}
}

func timeout(timer *<-chan time.Time, maxIdleTime time.Duration, countdown func() bool) *<-chan time.Time {
	if countdown() {
		newTimer := time.After(maxIdleTime)
		timer = &newTimer
	}
	return timer
}
