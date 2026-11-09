package counter

import (
	"sync"
	"sync/atomic"
)

type TrafficCounter struct {
	Counters sync.Map
}

type TrafficStorage struct {
	UpCounter   atomic.Int64
	DownCounter atomic.Int64
	UpStats     XrayTrafficCounter
	DownStats   XrayTrafficCounter
}

func newTrafficStorage() *TrafficStorage {
	s := &TrafficStorage{}
	s.UpStats.V = &s.UpCounter
	s.DownStats.V = &s.DownCounter
	return s
}

func NewTrafficCounter() *TrafficCounter {
	return &TrafficCounter{}
}

func (c *TrafficCounter) GetCounter(uuid string) *TrafficStorage {
	if cts, ok := c.Counters.Load(uuid); ok {
		return cts.(*TrafficStorage)
	}
	newStorage := newTrafficStorage()
	if cts, loaded := c.Counters.LoadOrStore(uuid, newStorage); loaded {
		return cts.(*TrafficStorage)
	}
	return newStorage
}

func (c *TrafficCounter) Delete(uuid string) {
	c.Counters.Delete(uuid)
}
