package concurrency

import (
	"fmt"

	"github.com/Jeffail/tunny"
)

var (
	_ tunny.Worker = (*MuxWorker)(nil)
)

type WorkType int32
type WorkParams interface {
	GetType() WorkType
}

type Handler func(payload interface{}) interface{}
type HandlerMap map[WorkType]Handler
type MuxWorker struct {
	handlers HandlerMap
}

func NewMuxWorker(handlers HandlerMap) *MuxWorker {
	return &MuxWorker{
		handlers: handlers,
	}
}

func (w *MuxWorker) Process(payload interface{}) interface{} {
	params, ok := payload.(WorkParams)
	if !ok {
		return fmt.Errorf("payload is not workParams")
	}

	handler, ok := w.handlers[params.GetType()]
	if !ok {
		return fmt.Errorf("no handler for workType=%v", params.GetType())
	}

	return handler(payload)
}

func (w *MuxWorker) BlockUntilReady() {}
func (w *MuxWorker) Interrupt()       {}
func (w *MuxWorker) Terminate()       {}
