package dispatcher

import (
	"sync/atomic"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

// trafficFlushThreshold bounds how many bytes a connection may buffer before
// committing to the shared user/device counters. Batching keeps the shared
// atomics from being hit once per 8 KiB buffer on every concurrent connection.
const trafficFlushThreshold int64 = 32 * 1024

// trafficWriter counts downlink bytes for one connection. pending is private
// to the connection; only the flush commits to the shared counters.
type trafficWriter struct {
	user    *atomic.Int64
	ip      *atomic.Int64
	pending atomic.Int64
	writer  buf.Writer
}

func (w *trafficWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if n := int64(mb.Len()); n > 0 {
		if w.pending.Add(n) >= trafficFlushThreshold {
			w.flush()
		}
	}
	return w.writer.WriteMultiBuffer(mb)
}

func (w *trafficWriter) flush() {
	n := w.pending.Swap(0)
	if n == 0 {
		return
	}
	if w.user != nil {
		w.user.Add(n)
	}
	if w.ip != nil {
		w.ip.Add(n)
	}
}

func (w *trafficWriter) Flush() {
	w.flush()
}

func (w *trafficWriter) Close() error {
	w.flush()
	return common.Close(w.writer)
}

func (w *trafficWriter) Interrupt() {
	w.flush()
	common.Interrupt(w.writer)
}
