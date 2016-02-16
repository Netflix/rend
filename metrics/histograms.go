// Copyright 2015 Netflix, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"math"
	"sync"
	"sync/atomic"
)

const (
	numHists = 1024
	buflen   = 0x7FFF // max index, 32769 entries
)

var (
	hnames    = make([]string, numHists)
	hists     = make([]*hist, numHists)
	curHistID = new(uint32)
)

func init() {
	// start at "-1" so the first ID is 0
	atomic.StoreUint32(curHistID, 0xFFFFFFFF)
}

// The hist struct holds a primary and secondary data structure so the reader of
// the histograms will get to read the data out while new observations are made.
// As well, pulling and resetting the histogram does not require a malloc in the
// path of pulling the data, and the large circular buffers can be reused.
type hist struct {
	lock sync.RWMutex
	prim *hdat
	sec  *hdat
}
type hdat struct {
	count *uint64
	kept  *uint64
	min   *uint64
	max   *uint64
	buf   []uint64
}

func newHist() *hist {
	return &hist{
		prim: newHdat(),
		sec:  newHdat(),
	}
}
func newHdat() *hdat {
	ret := &hdat{
		count: new(uint64),
		kept:  new(uint64),
		min:   new(uint64),
		max:   new(uint64),
		buf:   make([]uint64, buflen+1),
	}
	atomic.StoreUint64(ret.min, math.MaxUint64)
	return ret
}

func AddHistogram(name string) uint32 {
	idx := atomic.AddUint32(curHistID, 1)

	if idx >= numHists {
		panic("Too many histograms")
	}

	hnames[idx] = name
	hists[idx] = newHist()

	return idx
}

func ObserveHist(id uint32, value uint64) {
	h := hists[id]

	// We lock here to ensure that the min and max values are true to this time
	// period, meaning extractAndReset won't pull the data out from under us
	// while the current observation is being compared. Otherwise, min and max
	// could come from the previous period on the next read.
	h.lock.RLock()
	defer h.lock.RUnlock()

	// Set max and min (if needed) in an atomic fashion
	for {
		max := atomic.LoadUint64(h.prim.max)
		if value < max || atomic.CompareAndSwapUint64(h.prim.max, max, value) {
			break
		}
	}
	for {
		min := atomic.LoadUint64(h.prim.min)
		if value > min || atomic.CompareAndSwapUint64(h.prim.min, min, value) {
			break
		}
	}

	// Sample, keep every 4th observation
	if c := atomic.AddUint64(h.prim.count, 1) & 0x3; c > 0 {
		return
	}

	// Get the current index as the count % buflen
	idx := atomic.AddUint64(h.prim.kept, 1) & buflen

	// Add observation
	h.prim.buf[idx] = value
}

func getAllHistograms() map[string]*hdat {
	n := int(atomic.LoadUint32(curHistID))

	ret := make(map[string]*hdat)

	for i := 0; i < n; i++ {
		ret[hnames[i]] = extractAndReset(hists[i])
	}

	return ret
}

func extractAndReset(h *hist) *hdat {
	h.lock.Lock()

	// flip and reset the count
	temp := h.prim
	h.prim = h.sec
	h.sec = temp

	atomic.StoreUint64(h.prim.count, 0)
	atomic.StoreUint64(h.prim.kept, 0)
	atomic.StoreUint64(h.prim.max, 0)
	atomic.StoreUint64(h.prim.min, math.MaxUint64)

	h.lock.Unlock()

	return h.sec
}
