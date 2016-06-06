package pool

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"unsafe"
)

type FixedSizeBufferPool struct {
	buf      []byte
	bufsize  int
	count    uint64
	lenmask  uint64
	_        [7]uint64
	readIdx  uint64
	_        [7]uint64
	writeIdx uint64
	_        [7]uint64
}

// NewFixedSizeBufferPool creates a pool of buffers that can be shared among
// many goroutines. It provides slices of size `bufsize` out of a pool of `2^scale`
// slices. Returned slices should be carefully used and not written past their
// given length or data corrupton will occur.
func NewFixedSizeBufferPool(bufsize int, scale uint8) *FixedSizeBufferPool {
	count := uint64(1) << scale

	return &FixedSizeBufferPool{
		buf:     make([]byte, uint64(bufsize)*count),
		bufsize: bufsize,
		count:   count,
		lenmask: count - 1,
		readIdx: 0xFFFFFFFFFFFFFFFF, // start at "-1" to overflow to 0
	}
}

func (p *FixedSizeBufferPool) Get() ([]byte, uint64) {
	idx := atomic.AddUint64(&p.readIdx, 1)
	realIdx := (idx & p.lenmask) * uint64(p.bufsize)

	bufHeader := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&p.buf[realIdx])),
		Len:  p.bufsize,
		Cap:  p.bufsize,
	}

	// Wait for writers to catch up enough before returning
	for atomic.LoadUint64(&p.writeIdx)+p.count <= idx {
		runtime.Gosched()
	}

	outbuf := *(*[]byte)(unsafe.Pointer(&bufHeader))

	return outbuf, idx
}

func (p *FixedSizeBufferPool) Put(idx uint64) {
	// Get in line, waiting for the previous buffer to be put in before continuing
	//
	//    _ note that this ! is very important as we want to wait on NOT swapped
	//   /
	//  |
	//  v
	for !atomic.CompareAndSwapUint64(&p.writeIdx, idx, idx+1) {
		runtime.Gosched()
	}
}
