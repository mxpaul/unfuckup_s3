package worker

import (
//
)

type WorkerTask struct {
	Line      uint64
	Id        string
	FailCount uint32
}

type WorkResult struct {
	Task WorkerTask
	Err  error
}

type Worker struct {
	InputChannel  chan WorkerTask
	ResultChannel chan WorkResult
	//ControlChannel chan struct{}
	RipChannel chan struct{}
	Callback   func(WorkerTask) error
}

func (w *Worker) Start() {
	w.InputChannel = make(chan WorkerTask)
	w.ResultChannel = make(chan WorkResult)
	//w.ControlChannel = make(chan struct{})
	w.RipChannel = make(chan struct{}, 1)
	go func() {
		for task := range w.InputChannel {
			err := w.Callback(task)
			result := WorkResult{Task: task, Err: err}
			w.ResultChannel <- result
		}
		close(w.ResultChannel)
		w.RipChannel <- struct{}{}
		close(w.RipChannel)
		//for {
		//	select {
		//	case task := <-w.InputChannel:
		//		err := w.Callback(task)
		//		result := WorkResult{Task: task, Err: err}
		//		w.ResultChannel <- result
		//	case <-w.ControlChannel:
		//		w.RipChannel <- struct{}{}
		//		close(w.RipChannel)
		//		return
		//	}
		//}
	}()
}

func (w *Worker) StopAsync() {
	//w.ControlChannel <- struct{}{}
	//close(w.ControlChannel)
	close(w.InputChannel)
}

func (w *Worker) StopBlocking() {
	w.StopAsync()
	<-w.RipChannel
}
