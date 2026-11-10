package dispatcher

import (
	sync "sync"
	"sync/atomic"

	"github.com/wyx2685/v2node/limiter"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

type ManagedWriter struct {
	writer      buf.Writer
	manager     *LinkManager
	userCounter *atomic.Int64
	ipCounter   *atomic.Int64
	pending     atomic.Int64
	peer        *trafficWriter
	ipState     *limiter.IPState
	releaseOnce sync.Once
}

func (w *ManagedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if n := int64(mb.Len()); n > 0 {
		if w.pending.Add(n) >= trafficFlushThreshold {
			w.flush()
		}
	}
	return w.writer.WriteMultiBuffer(mb)
}

func (w *ManagedWriter) flush() {
	n := w.pending.Swap(0)
	if n == 0 {
		return
	}
	if w.userCounter != nil {
		w.userCounter.Add(n)
	}
	if w.ipCounter != nil {
		w.ipCounter.Add(n)
	}
}

// Flush commits locally buffered counters. It is called before traffic
// snapshots so that bytes not yet reaching the flush threshold are not lost.
func (w *ManagedWriter) Flush() {
	w.flush()
	if w.peer != nil {
		w.peer.flush()
	}
}

func (w *ManagedWriter) releaseIP() {
	w.releaseOnce.Do(func() {
		if w.ipState != nil {
			w.ipState.Release()
		}
	})
}

func (w *ManagedWriter) Close() error {
	w.Flush()
	w.releaseIP()
	if w.manager != nil {
		w.manager.RemoveWriter(w)
	}
	return common.Close(w.writer)
}

func (w *ManagedWriter) Interrupt() {
	w.Flush()
	w.releaseIP()
	if w.manager != nil {
		w.manager.RemoveWriter(w)
	}
	common.Interrupt(w.writer)
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
		// Flush both directions before releasing the device reference.
		w.Flush()
		if f, ok := r.(interface{ Flush() }); ok {
			f.Flush()
		}
		common.Close(w)
		common.Interrupt(r)
	}
}

// Flush commits buffered counters for every active link.
func (m *LinkManager) Flush() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for w, r := range m.links {
		w.Flush()
		if f, ok := r.(interface{ Flush() }); ok {
			f.Flush()
		}
	}
}
