package dispatcher

import (
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

var _ buf.TimeoutReader = (*CounterReader)(nil)

type CounterReader struct {
	Reader    buf.TimeoutReader
	Counter   *atomic.Int64
	IPCounter *atomic.Int64
	pending   atomic.Int64
}

func (c *CounterReader) add(n int64) {
	if n <= 0 {
		return
	}
	if c.pending.Add(n) >= trafficFlushThreshold {
		c.flush()
	}
}

func (c *CounterReader) flush() {
	n := c.pending.Swap(0)
	if n == 0 {
		return
	}
	if c.Counter != nil {
		c.Counter.Add(n)
	}
	if c.IPCounter != nil {
		c.IPCounter.Add(n)
	}
}

func (c *CounterReader) Flush() {
	c.flush()
}

func (c *CounterReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	mb, err := c.Reader.ReadMultiBufferTimeout(timeout)
	if err != nil {
		c.flush()
		return nil, err
	}
	if mb.Len() > 0 {
		c.add(int64(mb.Len()))
	}
	return mb, nil
}

func (c *CounterReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := c.Reader.ReadMultiBuffer()
	if err != nil {
		c.flush()
		return nil, err
	}
	if mb.Len() > 0 {
		c.add(int64(mb.Len()))
	}
	return mb, nil
}

func (c *CounterReader) Interrupt() {
	c.flush()
	common.Interrupt(c.Reader)
}

func (c *CounterReader) Close() error {
	c.flush()
	return common.Close(c.Reader)
}
