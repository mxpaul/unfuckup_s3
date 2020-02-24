package cmd

import (
	"context"
	//"errors"
	"fmt"
	"io/ioutil"
	"log"
	//"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/mxpaul/unfuckup_s3/generator"
	"github.com/mxpaul/unfuckup_s3/worker"
	"github.com/mxpaul/unfuckup_s3/worker/pool"
)

const (
	defaultInputFile            = "testdata/file-id-5m.txt"
	defaultOffset               = uint64(0)
	defaultLimit                = uint64(1)
	defaultValueChannelCapacity = uint64(1024)
	defaultErrorChannelCapacity = uint64(0)
	defaultMaxParallel          = uint64(100)
	defaultStatAfterLines       = uint64(100000)
	defaultStatAfterSeconds     = uint64(60)
)

func IsFileExist(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}

	return true, nil
}

func OpenInputFile(path string) (*os.File, error) {
	if exist, err := IsFileExist(path); err != nil || !exist {
		return nil, err
	}
	return os.Open(path)
}

func NewGeneratorFromConfig(config *viper.Viper) *generator.Generator {
	gen := &generator.Generator{
		Limit:                config.GetUint64("s3.generator.limit"),
		Offset:               config.GetUint64("s3.generator.offset"),
		ValueChannelCapacity: config.GetUint64("s3.generator.value_channel_capacity"),
		ErrorChannelCapacity: config.GetUint64("s3.generator.error_channel_capacity"),
	}
	return gen
}

func NewWorkerPoolFromConfig(config *viper.Viper) *pool.WorkerPool {
	wp := &pool.WorkerPool{
		OutputChannelCapacity: config.GetUint64("s3.workerpool.output_channel_capacity"),
		MaxParallel:           config.GetUint64("s3.workerpool.max_parallel"),
	}
	return wp
}

//func ProcessFileID(task worker.WorkerTask) (err error) {
//	time.Sleep(1 * time.Millisecond)
//	if rand.Float32() > 0.9 {
//		err = errors.New("timeout")
//	}
//	return err
//}

type Stat struct {
	Success uint64
	Fail    uint64
}

func (s *Stat) AddSuccess() {
	atomic.AddUint64(&s.Success, 1)
}
func (s *Stat) AddFail() {
	atomic.AddUint64(&s.Fail, 1)
}

func (s *Stat) Dump() {
	log.Printf("[STAT] Success: %d Fail: %d", s.Success, s.Fail)
}

type S3APP struct {
	Backuper       *worker.BackupClient
	Restorer       *worker.AmazonRestorer
	FakeHTTPServer *httptest.Server
}

type Middleware func(http.HandlerFunc) http.HandlerFunc

func ChainMiddleware(h http.HandlerFunc, middleware ...Middleware) http.HandlerFunc {
	handler := h
	for i := len(middleware) - 1; i >= 0; i-- {
		handler = middleware[i](handler)
	}
	return handler
}

func (app *S3APP) StartFakeServerFromConfig(config *viper.Viper) {
	mux := http.NewServeMux()
	middleware := []Middleware{
		//func(next http.HandlerFunc) http.HandlerFunc {
		//	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//		log.Printf("fake server got request: %+v", r.URL)
		//		next.ServeHTTP(w, r)
		//	})
		//},
	}

	backupHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		fmt.Fprint(w, strings.Repeat("0", 10240))
	})
	restoreHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if _, err := ioutil.ReadAll(r.Body); err == nil {
		}
	})
	mux.Handle("/backup/", ChainMiddleware(backupHandler, middleware...))
	mux.Handle("/restore/", ChainMiddleware(restoreHandler, middleware...))
	app.FakeHTTPServer = httptest.NewTLSServer(mux) // Server started
	app.Backuper = &worker.BackupClient{
		BackupUrlPrefix: fmt.Sprintf("%s/backup/", app.FakeHTTPServer.URL),
		Client:          app.FakeHTTPServer.Client(),
	}
	app.Restorer = &worker.AmazonRestorer{
		UrlPrefix: fmt.Sprintf("%s/restore/", app.FakeHTTPServer.URL),
		Client:    app.FakeHTTPServer.Client(),
	}
}

func (app *S3APP) InitClientsFromConfigOrDie(config *viper.Viper) {
	backup_url_prefix := config.GetString("s3.backup.url_prefix")
	restore_url_prefix := config.GetString("s3.restore.url_prefix")
	if backup_url_prefix == "" {
		log.Fatalf("s3.backup.url_prefix not set")
	}
	if restore_url_prefix == "" {
		log.Fatalf("s3.restore.url_prefix not set")
	}

	client := &http.Client{}

	app.Backuper = &worker.BackupClient{
		BackupUrlPrefix: backup_url_prefix,
		Client:          client,
	}
	app.Restorer = &worker.AmazonRestorer{
		UrlPrefix: restore_url_prefix,
		Client:    client,
	}
}

func (app *S3APP) FilePrecessCallback() worker.WorkerCallback {
	return func(task worker.WorkerTask) (err error) {
		body, err := app.Backuper.RequestBackupBody(task.Id)
		if err != nil {
			log.Printf("backuper: %s", err)
			return err
		}
		return app.Restorer.PutObjectFromReader(task.Id, body)
	}
}

func s3Run(cmd *cobra.Command, args []string) {
	log.Print("Start application")
	config := viper.GetViper()
	inputFileName := config.GetString("s3.input")
	log.Printf(`input file: "%s"`, inputFileName)

	inputfd, err := OpenInputFile(inputFileName)
	if err != nil {
		log.Fatalf("input file %s open error: %s", inputFileName, err)
	}
	defer inputfd.Close()

	ctx, genShutdown := context.WithCancel(context.Background())

	app := S3APP{}
	if config.GetBool("s3.fakeserver.use_fake_server") {
		app.StartFakeServerFromConfig(config)
	} else {
		app.InitClientsFromConfigOrDie(config)
	}

	gen := NewGeneratorFromConfig(config)
	gen.Init(inputfd)
	gen.Go(ctx)

	pool := NewWorkerPoolFromConfig(config)
	pool.InputChannel = gen.ValueChannel
	//pool.Go(ProcessFileID)
	pool.Go(app.FilePrecessCallback())

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	stat := Stat{}
	go func() {
		for {
			time.Sleep(time.Duration(config.GetUint64("s3.stat.after_seconds")) * time.Second)
			stat.Dump()
		}
	}()

	readCount := uint64(0)
	var Break bool
	var defaultWorkResult worker.WorkResult
	for !Break {
		select {
		case msg, can_read := <-gen.ErrorChannel:
			if can_read {
				log.Printf("[ERR] Line %d: %s", msg.Line, msg.Err)
			}
		case res, open := <-pool.OutputChannel:
			if !open {
				Break = true
				break
			}
			if res == defaultWorkResult {
				log.Printf("WTF! Default value from open channel!")
				break
			}
			//log.Printf("Done task %+v", res.Task)
			if res.Err == nil {
				stat.AddSuccess()
			} else {
				stat.AddFail()
			}
			readCount++
			if readCount%viper.GetUint64("s3.stat.after_lines") == 0 {
				stat.Dump()
			}
		case <-gen.DoneChannel:
			pool.StopAsync()
		case GotSignal := <-sigchan:
			log.Print("")
			log.Printf("Got signal %v", GotSignal)
			pool.StopAsync() // pool should ignore new tasks from generator
			genShutdown()
		}
	}
	gen.WG.Wait()

	stat.Dump()
	if app.FakeHTTPServer != nil {
		app.FakeHTTPServer.Close()
	}
	log.Printf("exit after reading %d lines", readCount)
}
