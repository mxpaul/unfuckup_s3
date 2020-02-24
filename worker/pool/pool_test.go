package pool

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/mxpaul/unfuckup_s3/worker"
)

func init() {
	// Fuck imported not used checks
	_ = fmt.Printf
	_ = log.Printf
}

func AlwaysOK(worker.WorkerTask) error {
	time.Sleep(1 * time.Microsecond)
	return nil
}

func AlwaysOKDelayedFor(duration time.Duration) worker.WorkerCallback {
	return func(worker.WorkerTask) error {
		time.Sleep(duration)
		return nil
	}
}

func TestInitWorkerPool(t *testing.T) {
	wp := WorkerPool{
		MaxParallel:           3,
		OutputChannelCapacity: 10,
	}
	wp.Init(AlwaysOK)
	assert.Equal(t, len(wp.worker), int(wp.MaxParallel), "Init creats worker slice sized as MaxParallel")
	if assert.NotNil(t, wp.OutputChannel, "Output channel created") {
		assert.Equal(t, cap(wp.OutputChannel), int(wp.OutputChannelCapacity), "Output channel capacity match")
	}
	//if assert.NotNil(t, wp.ControlChannel, "Control channel created") {
	//	assert.Equal(t, cap(wp.ControlChannel), 1, "Control channel capacity 1 for non-blocking send")
	//}
	if assert.NotNil(t, wp.RipChannel, "workerpool RIP channel created") {
		assert.Equal(t, cap(wp.RipChannel), 1, "RIP channel capacity 1 for non-blocking send")
	}
	if assert.NotNil(t, wp.FanInRipChannel, "FanIn RIP channel created") {
		assert.Equal(t, cap(wp.FanInRipChannel), 1, "fan-in RIP channel capacity 1 for non-blocking send")
	}
	//cb := AlwaysOK
	//for i, worker := range wp.worker {
	//	workerCB := worker.Callback
	//	assert.Equalf(t, &cb, &workerCB, "callback assigned to worker %d", i)
	//}
}

func TestFanIn(t *testing.T) {
	wp := WorkerPool{
		MaxParallel: 3,
	}
	wp.Init(AlwaysOK)
	// FanIn selects from workers output channel, so start workers first
	for _, w := range wp.worker {
		w.Start()
	}
	go wp.FanIn()
	expected := []worker.WorkResult{
		worker.WorkResult{Task: worker.WorkerTask{Line: 1, Id: "111"}, Err: nil},
		worker.WorkResult{Task: worker.WorkerTask{Line: 2, Id: "222"}, Err: nil},
		worker.WorkResult{Task: worker.WorkerTask{Line: 3, Id: "333"}, Err: nil},
	}
	for i, w := range wp.worker {
		w.InputChannel <- expected[i].Task
		close(w.InputChannel)
	}
	got := make([]worker.WorkResult, 0, 3)
	for res := range wp.OutputChannel {
		got = append(got, res)
	}

	Riped := func() bool {
		select {
		case <-wp.FanInRipChannel:
			return true
		default:
			return false
		}
	}
	assert.Eventually(t, Riped, 10*time.Millisecond, 1*time.Millisecond)

	assert.ElementsMatch(t, got, expected, "expected results read from output channel")

}

func TestFanOut(t *testing.T) {
	wp := WorkerPool{
		MaxParallel:          3,
		InputChannelCapacity: 3,
	}
	wp.Init(AlwaysOK)
	for _, w := range wp.worker {
		w.Start()
	}
	go wp.FanOut()
	go wp.FanIn()
	wp.InputChannel <- worker.WorkerTask{Line: 1, Id: "111"}
	wp.InputChannel <- worker.WorkerTask{Line: 2, Id: "222"}
	wp.InputChannel <- worker.WorkerTask{Line: 3, Id: "333"}

	got := make([]worker.WorkResult, 0, 3)
	ReadToGo := 3
	ReadEveryResult := func() bool {
		select {
		case res := <-wp.OutputChannel:
			got = append(got, res)
			ReadToGo--
		default:
		}
		return ReadToGo == 0
	}
	assert.Eventually(t, ReadEveryResult, 100*time.Millisecond, 1*time.Millisecond)

	close(wp.InputChannel)
	//wp.ControlChannel <- struct{}{} // Signal FanOut to finish gracefully
	Riped := func() bool {
		select {
		case <-wp.RipChannel:
			return true
		default:
			return false
		}
	}
	assert.Eventually(t, Riped, 10*time.Millisecond, 1*time.Millisecond)
	expected := []worker.WorkResult{
		worker.WorkResult{Task: worker.WorkerTask{Line: 1, Id: "111"}, Err: nil},
		worker.WorkResult{Task: worker.WorkerTask{Line: 2, Id: "222"}, Err: nil},
		worker.WorkResult{Task: worker.WorkerTask{Line: 3, Id: "333"}, Err: nil},
	}
	assert.ElementsMatch(t, got, expected, "expected results read from output channel")

	//// Generator may put additional tasks into InputChannel but it will be ignored
	//wp.InputChannel <- worker.WorkerTask{Line: 1, Id: "111"}
	//wp.InputChannel <- worker.WorkerTask{Line: 2, Id: "222"}
	//wp.InputChannel <- worker.WorkerTask{Line: 3, Id: "333"}
	//wp.InputChannel <- worker.WorkerTask{Line: 3, Id: "333"} // Block if FanOut not reading
	//close(wp.InputChannel)
}

func TestParallelTaskProcessing(t *testing.T) {
	jobDelay := 10 * time.Millisecond
	jobCount := 27
	wp := WorkerPool{
		MaxParallel:          3,
		InputChannelCapacity: uint64(jobCount),
	}
	callback := AlwaysOKDelayedFor(jobDelay)
	wp.Go(callback)
	task := worker.WorkerTask{Line: 1, Id: "111"}

	for i := 0; i < jobCount; i++ {
		wp.InputChannel <- task
	}

	start := time.Now()
	gotCount := 0
	ReadEveryResult := func() bool {
		select {
		case <-wp.OutputChannel:
			gotCount++
		default:
		}
		return gotCount >= jobCount
	}
	assert.Eventually(t, ReadEveryResult, 100*jobDelay, 1*time.Millisecond)
	finish := time.Now()

	wp.StopBlocking()
	assert.Equal(t, jobCount, gotCount, "all tasks processed")
	assert.WithinDuration(t, finish, start, 10*jobDelay, "parallel execution")
}
