package cmd

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/mxpaul/unfuckup_s3/generator"
)

const (
	defaultInputFile            = "testdata/file-id-5m.txt"
	defaultOffset               = uint64(0)
	defaultLimit                = uint64(1)
	defaultValueChannelCapacity = uint64(1024)
	defaultErrorChannelCapacity = uint64(0)
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

func s3Run(cmd *cobra.Command, args []string) {
	log.Print("Start application")
	inputFileName := viper.GetString("s3.input")
	log.Printf(`input file: "%s"`, inputFileName)

	inputfd, err := OpenInputFile(inputFileName)
	if err != nil {
		log.Fatalf("input file %s open error: %s", inputFileName, err)
	}
	defer inputfd.Close()

	ctx, Shutdown := context.WithCancel(context.Background())

	gen := NewGeneratorFromConfig(viper.GetViper())
	//log.Printf("generator: %+v", gen)
	gen.Init(inputfd)
	gen.Go(ctx)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	log.Print("read generator results")
	readCount := uint64(0)
	var Exiting bool
	for !Exiting {
		select {
		case msg, can_read := <-gen.ValueChannel:
			if can_read {
				_ = msg
				readCount++
				//log.Printf("Line %d: %s", msg.Line, msg.Id)
			}
		case msg, can_read := <-gen.ErrorChannel:
			if can_read {
				log.Printf("[ERR] Line %d: %s", msg.Line, msg.Err)
			}
		case <-gen.DoneChannel:
			Exiting = true
		case GotSignal := <-sigchan:
			Exiting = true
			log.Print("")
			log.Printf("Got signal %v", GotSignal)
			Shutdown()
		}
	}
	gen.WG.Wait()

	log.Printf("exit after reading %d lines", readCount)
}
