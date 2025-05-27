package event

import (
	"sync"
	"time"
)

type Queue struct {
	done        sync.WaitGroup
	events      chan ExecutableEvent
	globalHooks []func(BaseEventInfo, any)
}

type ExecutableEvent interface {
	Trigger()
	EventInfo() BaseEventInfo
	PayloadData() any
}

func NewEventQueue(buffer int) *Queue {
	return &Queue{
		events: make(chan ExecutableEvent, buffer),
	}
}

func (q *Queue) Start() {
	q.done.Add(1)
	go func() {
		defer q.done.Done()
		for e := range q.events {
			e.Trigger()

			info, payload := e.EventInfo(), e.PayloadData()
			for _, gh := range q.globalHooks {
				gh(info, payload)
			}

			time.Sleep(1 * time.Second)
		}
	}()
}

func (q *Queue) Add(e ExecutableEvent) {
	q.events <- e
}

func (q *Queue) Stop() {
	close(q.events)
	q.done.Wait()
}

func (q *Queue) AddGlobalHook(h func(BaseEventInfo, any)) {
	q.globalHooks = append(q.globalHooks, h)
}
