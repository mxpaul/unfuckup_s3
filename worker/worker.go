package worker

type WorkerCallback func(WorkerTask) error

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
	RipChannel    chan struct{}
	Callback      func(WorkerTask) error
	Ident         string
}

func (w *Worker) Start() {
	w.InputChannel = make(chan WorkerTask)
	w.ResultChannel = make(chan WorkResult)
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
	}()
}

func (w *Worker) StopAsync() {
	close(w.InputChannel)
}

func (w *Worker) StopBlocking() {
	w.StopAsync()
	<-w.RipChannel
}
