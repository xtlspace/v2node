package dispatcher

import (
	sync "sync"
	"sync/atomic"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/features/stats"
)

type ManagedWriter struct {
	writer      buf.Writer
	manager     *LinkManager
	userCounter stats.Counter
	ipCounter   *atomic.Int64
}

func (w *ManagedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	n := int64(mb.Len())
	if w.userCounter != nil {
		w.userCounter.Add(n)
	}
	if w.ipCounter != nil {
		w.ipCounter.Add(n)
	}
	return w.writer.WriteMultiBuffer(mb)
}

func (w *ManagedWriter) Close() error {
	w.manager.RemoveWriter(w)
	return common.Close(w.writer)
}

type LinkManager struct {
	links  map[*ManagedWriter]buf.Reader
	mu     sync.RWMutex
	closed bool
}

func (m *LinkManager) AddLink(writer *ManagedWriter, reader buf.Reader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.links[writer] = reader
	}
}

func (m *LinkManager) RemoveWriter(writer *ManagedWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		delete(m.links, writer)
	}
}

func (m *LinkManager) CloseAll() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true

	links := m.links
	m.links = make(map[*ManagedWriter]buf.Reader)
	m.mu.Unlock()

	for w, r := range links {
		common.Close(w.writer)
		common.Interrupt(r)
	}
}
