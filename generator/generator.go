package generator

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"regexp"
	"sync"
)

type GeneratorError struct {
	Line uint64
	Err  error
}

type GeneratorValue struct {
	Line uint64
	Id   string
}

type Generator struct {
	ValueChannel         chan GeneratorValue
	ErrorChannel         chan GeneratorError
	DoneChannel          chan struct{}
	id_src               io.Reader
	Offset               uint64
	Limit                uint64
	ValueChannelCapacity uint64
	ErrorChannelCapacity uint64
	WG                   sync.WaitGroup
}

//func NewGenerator(id_src io.Reader) *Generator {
//	gen := &Generator{
//		ValueChannel: make(chan string),
//		ErrorChannel: make(chan error),
//		id_src:       id_src,
//	}
//	return gen
//}

func (gen *Generator) Init(id_src io.Reader) {
	gen.ValueChannel = make(chan GeneratorValue, gen.ValueChannelCapacity)
	gen.ErrorChannel = make(chan GeneratorError, gen.ErrorChannelCapacity)
	gen.DoneChannel = make(chan struct{}, 1)
	//gen.WG = sync.WaitGroup{}
	gen.id_src = id_src
}

func (gen *Generator) Go(ctx context.Context) {
	invalidFileIdRE := regexp.MustCompile(`\s`)
	gen.WG.Add(1)
	go func() {
		scaner := bufio.NewScanner(gen.id_src)
		defer func() {
			close(gen.ValueChannel)
			close(gen.ErrorChannel)
			gen.DoneChannel <- struct{}{}
			close(gen.DoneChannel)
			gen.WG.Done()
		}()
		var position uint64
		for scaner.Scan() { // FIXME: тут есть шанс надолго заблокироваться
			select {
			case <-ctx.Done():
				log.Printf("generator interrupted after line %d", position)
				return
			default:
			}
			position++
			if position <= gen.Offset {
				continue
			}
			if gen.Limit > 0 && position > gen.Offset+gen.Limit {
				break
			}
			text := scaner.Text()
			if len(text) == 0 {
				continue
			}
			if invalidFileIdRE.Match([]byte(text)) {
				gen.ErrorChannel <- GeneratorError{Line: position, Err: fmt.Errorf("file id may not contain spaces")}
				return
			}

			gen.ValueChannel <- GeneratorValue{Line: position, Id: text}
		}
		if err := scaner.Err(); err != nil {
			gen.ErrorChannel <- GeneratorError{Err: fmt.Errorf("scan error: %s", err)}
			return
		}
	}()
}
