package worker

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	ValidTask              = WorkerTask{Line: 1, Id: "123"}
	ValidTaskResultSuccess = WorkResult{Task: ValidTask, Err: nil}
	ValidTaskResultFail    = WorkResult{Task: ValidTask, Err: fmt.Errorf("fail")}
)

func AlwaysOK(WorkerTask) error {
	time.Sleep(1 * time.Microsecond)
	return nil
}

func AlwaysFail(WorkerTask) error {
	time.Sleep(1 * time.Microsecond)
	return ValidTaskResultFail.Err
}

func TestOneTaskSuccess(t *testing.T) {

	w := Worker{
		Callback: AlwaysOK,
	}
	w.Start()
	task := ValidTask
	w.InputChannel <- task
	select {
	case res := <-w.ResultChannel:
		assert.Equal(t, ValidTaskResultSuccess, res, "result returned")
	}
	w.StopAsync()
	<-w.RipChannel
}

func TestOneTaskFail(t *testing.T) {

	w := Worker{
		Callback: AlwaysFail,
	}
	w.Start()
	task := ValidTask
	w.InputChannel <- task
	select {
	case res := <-w.ResultChannel:
		assert.Equal(t, ValidTaskResultFail, res, "result returned")
	}
	w.StopAsync()
	<-w.RipChannel
}
