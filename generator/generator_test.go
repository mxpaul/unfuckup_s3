package generator

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

//func init() {
//	runtime.GOMAXPROCS(1)
//}

func TestRead3Lines(t *testing.T) {
	inputReader := strings.NewReader("1\n2\n3")
	gen := &Generator{}
	gen.Init(inputReader)
	ctx, _ := context.WithCancel(context.Background())
	gen.Go(ctx)
	expect := []GeneratorValue{GeneratorValue{1, "1"}, GeneratorValue{2, "2"}, GeneratorValue{3, "3"}}
	got := make([]GeneratorValue, 0, 3)
	for line := range gen.ValueChannel {
		got = append(got, line)
	}
	assert.Equal(t, got, expect, "all tree line read from reader and pushed to values channel")

}

type TestCase struct {
	Instance          *Generator
	Input             string
	WantValue         []GeneratorValue
	WantError         []GeneratorError
	Desc              string
	CancelAfterNLoops uint64
}

func CheckTestCases(t *testing.T, tests []TestCase) {
	for _, test := range tests {
		inputReader := strings.NewReader(test.Input)
		gen := test.Instance
		if gen == nil {
			gen = &Generator{}
		}
		gen.Init(inputReader)
		//log.Printf("generator values cap: %+v", cap(gen.ValueChannel))
		ctx, ctxCancel := context.WithCancel(context.Background())
		gen.Go(ctx)
		gotvalue := make([]GeneratorValue, 0, len(test.WantValue))
		goterror := make([]GeneratorError, 0, len(test.WantError))
		loopCount := uint64(0)
		log.SetPrefix(test.Desc)
	GENERATOR_LOOP:
		for {
			if test.CancelAfterNLoops > 0 && loopCount == test.CancelAfterNLoops {
				ctxCancel()
				runtime.Gosched()
			}
			loopCount++
			select {
			case msg, ok := <-gen.ValueChannel:
				if ok {
					gotvalue = append(gotvalue, msg)
				}
			case msg, ok := <-gen.ErrorChannel:
				if ok {
					goterror = append(goterror, msg)
				}
			case <-gen.DoneChannel:
				break GENERATOR_LOOP
			}
		}
		for msg := range gen.ValueChannel {
			gotvalue = append(gotvalue, msg)
		}
		for msg := range gen.ErrorChannel {
			goterror = append(goterror, msg)
		}
		gen.WG.Wait()
		assert.Equal(t, test.WantValue, gotvalue, test.Desc)
		if test.WantError != nil {
			assert.Equal(t, test.WantError, goterror, "errors match when %s", test.Desc)
		} else {
			assert.Equal(t, len(goterror), 0, "no errors when %s", test.Desc)
		}
	}
}

func TestTableReadLines(t *testing.T) {
	tests := []TestCase{
		{Desc: "three single-char lines",
			Input:     "1\n2\n3",
			WantValue: []GeneratorValue{GeneratorValue{1, "1"}, GeneratorValue{2, "2"}, GeneratorValue{3, "3"}},
		},
		{Desc: "lines have spaces",
			Input:     "11\n2 2\n3",
			WantValue: []GeneratorValue{GeneratorValue{1, "11"}},
			WantError: []GeneratorError{GeneratorError{2, fmt.Errorf("file id may not contain spaces")}},
		},
		{Desc: "newline at the end",
			Input:     "1\n2\n3\n",
			WantValue: []GeneratorValue{GeneratorValue{1, "1"}, GeneratorValue{2, "2"}, GeneratorValue{3, "3"}},
		},
		{Desc: "empty line in the middle",
			Input:     "1\n2\n\n3\n",
			WantValue: []GeneratorValue{GeneratorValue{1, "1"}, GeneratorValue{2, "2"}, GeneratorValue{4, "3"}},
		},
		{Desc: "offset 1",
			Instance:  &Generator{Offset: 1},
			Input:     "1\n2\n3\n",
			WantValue: []GeneratorValue{GeneratorValue{2, "2"}, GeneratorValue{3, "3"}},
		},
		{Desc: "offset 1 limit 1",
			Instance:  &Generator{Offset: 1, Limit: 1},
			Input:     "1\n2\n3\n",
			WantValue: []GeneratorValue{GeneratorValue{2, "2"}},
		},
		{Desc: "offset 0 limit 2",
			Instance:  &Generator{Offset: 0, Limit: 2},
			Input:     "1\n2\n3\n",
			WantValue: []GeneratorValue{GeneratorValue{1, "1"}, GeneratorValue{2, "2"}},
		},
		{Desc: "limit 0 ignored",
			Instance:  &Generator{Offset: 0, Limit: 0},
			Input:     "1\n2\n3\n",
			WantValue: []GeneratorValue{GeneratorValue{1, "1"}, GeneratorValue{2, "2"}, GeneratorValue{3, "3"}},
		},
		{Desc: "offset 1 but limit 0",
			Instance:  &Generator{Offset: 1, Limit: 0},
			Input:     "1\n2\n3\n",
			WantValue: []GeneratorValue{GeneratorValue{2, "2"}, GeneratorValue{3, "3"}},
		},
		{Desc: "buffered values channel",
			Instance:  &Generator{ValueChannelCapacity: 5},
			Input:     "1\n2\n3\n",
			WantValue: []GeneratorValue{GeneratorValue{1, "1"}, GeneratorValue{2, "2"}, GeneratorValue{3, "3"}},
		},
		{Desc: "buffered error channel",
			Instance:  &Generator{ErrorChannelCapacity: 2},
			Input:     "1\n2\n3\n",
			WantValue: []GeneratorValue{GeneratorValue{1, "1"}, GeneratorValue{2, "2"}, GeneratorValue{3, "3"}},
		},
		{Desc: "buffered both error and values channels",
			Instance:  &Generator{ValueChannelCapacity: 5, ErrorChannelCapacity: 2},
			Input:     "1\n2\n3\n",
			WantValue: []GeneratorValue{GeneratorValue{1, "1"}, GeneratorValue{2, "2"}, GeneratorValue{3, "3"}},
		},
		{Desc: "three single-char lines canceled after first line",
			CancelAfterNLoops: 1,
			Input:             "1\n2\n3",
			// FIXME: получаем два результата, первый send в канал не блокирует горутину
			WantValue: []GeneratorValue{GeneratorValue{1, "1"}, GeneratorValue{2, "2"}},
		},
	}
	CheckTestCases(t, tests)

}
