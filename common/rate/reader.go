package rate

import (
	"time"

	"github.com/xtls/xray-core/common/buf"
)

type Reader struct {
	reader  buf.TimeoutReader
	limiter *DynamicBucket
}

func NewRateLimitReader(reader buf.TimeoutReader, limiter *DynamicBucket) buf.TimeoutReader {
	return &Reader{
		reader:  reader,
		limiter: limiter,
	}
}

func (r *Reader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := r.reader.ReadMultiBuffer()
	r.wait(mb)
	return mb, err
}

func (r *Reader) ReadMultiBufferTimeout(duration time.Duration) (buf.MultiBuffer, error) {
	mb, err := r.reader.ReadMultiBufferTimeout(duration)
	r.wait(mb)
	return mb, err
}

func (r *Reader) wait(mb buf.MultiBuffer) {
	if mb.IsEmpty() {
		return
	}
	if limiter := r.limiter.Get(); limiter != nil {
		limiter.Wait(int64(mb.Len()))
	}
}
