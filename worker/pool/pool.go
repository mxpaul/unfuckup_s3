package pool

import (
	"fmt"
	"log"
	"sync"

	"github.com/mxpaul/unfuckup_s3/worker"
)

type WorkerPool struct {
	InputChannel          chan worker.WorkerTask
	OutputChannel         chan worker.WorkResult
	RipChannel            chan struct{}
	FanInRipChannel       chan struct{}
	MaxParallel           uint64
	worker                []*worker.Worker
	InputChannelCapacity  uint64
	OutputChannelCapacity uint64
}

func (wp *WorkerPool) Go(cb worker.WorkerCallback) {
	wp.Init(cb)
	for _, w := range wp.worker {
		w.Start()
	}
	go wp.FanOut()
	go wp.FanIn()
}

func (wp *WorkerPool) Init(cb worker.WorkerCallback) {
	wp.RipChannel = make(chan struct{}, 1)
	wp.FanInRipChannel = make(chan struct{}, 1)
	wp.InputChannel = make(chan worker.WorkerTask, wp.InputChannelCapacity)
	wp.OutputChannel = make(chan worker.WorkResult, wp.OutputChannelCapacity)

	wp.worker = make([]*worker.Worker, 0, wp.MaxParallel)
	for i := uint64(0); i < wp.MaxParallel; i++ {
		worker := &worker.Worker{Callback: cb, Ident: fmt.Sprintf("%d", i)}
		wp.worker = append(wp.worker, worker)
	}
}

func (wp *WorkerPool) FanOut() {
	var finish bool
	for i := uint64(0); !finish; i = (i + 1) % wp.MaxParallel {
		select {
		case task, ok := <-wp.InputChannel:
			if !ok {
				finish = true
				break
			}
			if task.Line == 0 {
				log.Printf("WTF!!! empty task from generator: open=%v", ok)
			} else {
				wp.worker[i].InputChannel <- task
			}
		}
	}
	for _, worker := range wp.worker {
		worker.StopAsync()
	}
	<-wp.FanInRipChannel
	wp.RipChannel <- struct{}{}
}

func (wp *WorkerPool) FanIn() {
	var wg sync.WaitGroup
	wg.Add(len(wp.worker))
	for i, w := range wp.worker {
		go func(w *worker.Worker, i int) {
			for resp := range w.ResultChannel {
				wp.OutputChannel <- resp
			}
			wg.Done()
		}(w, i)
	}
	wg.Wait()
	close(wp.OutputChannel)
	wp.FanInRipChannel <- struct{}{}
	close(wp.FanInRipChannel)
}

func (wp *WorkerPool) StopAsync() {
	close(wp.InputChannel)
}

func (wp *WorkerPool) StopBlocking() {
	wp.StopAsync()
	<-wp.RipChannel
}
