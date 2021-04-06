package examples

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/runnerhbc/workerpool/pool"
)

func TestFixedPool(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() - 2)
	//p, err := workerpool.NewFixedPool(3,
	p, err := pool.NewPool(3, 5,
		pool.WaitQueueSize(10),
		pool.MaxIdleTime(5*time.Second),
		//workerpool.Fair(true),
	)

	if err != nil {
		panic(err)
	}
	go func() {
		for i := 0; i < 20; i++ {
			id := strconv.Itoa(i)
			err = p.Submit(&pool.Task{Id: id, Run: func() (interface{}, error) {
				time.Sleep(6 * time.Second)
				return fmt.Sprintf("task finished:%s", id), nil
			}})
			//log.Printf("task %s submit\n", id)
		}
	}()
	resWG := sync.WaitGroup{}
	resWG.Add(1)
	for i := 0; i < 1; i++ {
		go func(id int) {
			resWG.Wait()
			for c := range p.Poll(20 * time.Second) {
				time.Sleep(5 * time.Second)
				r, e := c.Get()
				log.Println(id, r, e)
			}
			log.Println("finished", id)
		}(i)
		resWG.Done()
	}

	time.Sleep(10 * time.Second)
	p.ShutDown()
	//p.ShutDownNow()
	for i := 20; i < 40; i++ {
		id := strconv.Itoa(i)
		err = p.Execute(&pool.Task{Id: id, Run: func() (interface{}, error) {
			return fmt.Sprintf("task finished:%s", id), nil
		}})
		fmt.Println(err)
	}
	time.Sleep(time.Minute)
}
