package main

import (
	"log"

	"github.com/mxpaul/unfuckup_s3/cmd"
	//cmd "github.com/mxpaul/unfuckup_s3/app"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func main() {
	cmd.Execute()
}
