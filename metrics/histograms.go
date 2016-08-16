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
	maxNumHists     = 1024
	buflen          = 0x7FFF // max index, 32769 entries
	numAtlasBuckets = 276
)

var (
	hnames    = make([]string, maxNumHists)
	hsampled  = make([]bool, maxNumHists)
	hists     = make([]hist, maxNumHists)
	bhists    = make([]bhist, maxNumHists)
	curHistID = new(uint32)

	powerOf4Index = []int{0, 3, 14, 23, 32, 41, 50, 59, 68, 77, 86, 95, 104, 113, 122, 131, 140, 149, 158, 167, 176, 185, 194, 203, 212, 221, 230, 239, 248, 257, 266, 275}

	bucketValues = []int64{
		1, 2, 3, 4, 5,
		6, 7, 8, 9, 10,
		11, 12, 13, 14, 16,
		21, 26, 31, 36, 41,
		46, 51, 56, 64, 85,
		106, 127, 148, 169, 190,
		211, 232, 256, 341, 426,
		511, 596, 681, 766, 851,
		936, 1024, 1365, 1706, 2047,
		2388, 2729, 3070, 3411, 3752,
		4096, 5461, 6826, 8191, 9556,
		10921, 12286, 13651, 15016, 16384,
		21845, 27306, 32767, 38228, 43689,
		49150, 54611, 60072, 65536, 87381,
		109226, 131071, 152916, 174761, 196606,
		218451, 240296, 262144, 349525, 436906,
		524287, 611668, 699049, 786430, 873811,
		961192, 1048576, 1398101, 1747626, 2097151,
		2446676, 2796201, 3145726, 3495251, 3844776,
		4194304, 5592405, 6990506, 8388607, 9786708,
		11184809, 12582910, 13981011, 15379112, 16777216,
		22369621, 27962026, 33554431, 39146836, 44739241,
		50331646, 55924051, 61516456, 67108864, 89478485,
		111848106, 134217727, 156587348, 178956969, 201326590,
		223696211, 246065832, 268435456, 357913941, 447392426,
		536870911, 626349396, 715827881, 805306366, 894784851,
		984263336, 1073741824, 1431655765, 1789569706, 2147483647,
		2505397588, 2863311529, 3221225470, 3579139411, 3937053352,
		4294967296, 5726623061, 7158278826, 8589934591, 10021590356,
		11453246121, 12884901886, 14316557651, 15748213416, 17179869184,
		22906492245, 28633115306, 34359738367, 40086361428, 45812984489,
		51539607550, 57266230611, 62992853672, 68719476736, 91625968981,
		114532461226, 137438953471, 160345445716, 183251937961, 206158430206,
		229064922451, 251971414696, 274877906944, 366503875925, 458129844906,
		549755813887, 641381782868, 733007751849, 824633720830, 916259689811,
		1007885658792, 1099511627776, 1466015503701, 1832519379626, 2199023255551,
		2565527131476, 2932031007401, 3298534883326, 3665038759251, 4031542635176,
		4398046511104, 5864062014805, 7330077518506, 8796093022207, 10262108525908,
		11728124029609, 13194139533310, 14660155037011, 16126170540712, 17592186044416,
		23456248059221, 29320310074026, 35184372088831, 41048434103636, 46912496118441,
		52776558133246, 58640620148051, 64504682162856, 70368744177664, 93824992236885,
		117281240296106, 140737488355327, 164193736414548, 187649984473769, 211106232532990,
		234562480592211, 258018728651432, 281474976710656, 375299968947541, 469124961184426,
		562949953421311, 656774945658196, 750599937895081, 844424930131966, 938249922368851,
		1032074914605736, 1125899906842624, 1501199875790165, 1876499844737706, 2251799813685247,
		2627099782632788, 3002399751580329, 3377699720527870, 3752999689475411, 4128299658422952,
		4503599627370496, 6004799503160661, 7505999378950826, 9007199254740991, 10508399130531156,
		12009599006321321, 13510798882111486, 15011998757901651, 16513198633691816, 18014398509481984,
		24019198012642645, 30023997515803306, 36028797018963967, 42033596522124628, 48038396025285289,
		54043195528445950, 60047995031606611, 66052794534767272, 72057594037927936, 96076792050570581,
		120095990063213226, 144115188075855871, 168134386088498516, 192153584101141161, 216172782113783806,
		240191980126426451, 264211178139069096, 288230376151711744, 384307168202282325, 480383960252852906,
		576460752303423487, 672537544353994068, 768614336404564649, 864691128455135230, 960767920505705811,
		1056844712556276392, 1152921504606846976, 1537228672809129301, 1921535841011411626, 2305843009213693951,
		2690150177415976276, 3074457345618258601, 3458764513820540926, 3843071682022823251, 4227378850225105576,
		9223372036854775807,
	}
)

func init() {
	// start at "-1" so the first ID is 0
	atomic.StoreUint32(curHistID, 0)
}

// The hist struct holds a primary and secondary data structure so the reader of
// the histograms will get to read the data out while new observations are made.
// As well, pulling and resetting the histogram does not require a malloc in the
// path of pulling the data, and the large circular buffers can be reused.
type hist struct {
	lock sync.RWMutex
	prim hdat
	sec  hdat
}
type hdat struct {
	count uint64
	kept  uint64
	total uint64
	min   uint64
	max   uint64
	buf   []uint64
}

func newHist() hist {
	return hist{
		// read: primary and secondary data structures
		prim: newHdat(),
		sec:  newHdat(),
	}
}
func newHdat() hdat {
	ret := hdat{
		buf: make([]uint64, buflen+1),
	}
	atomic.StoreUint64(&ret.min, math.MaxUint64)
	return ret
}

type bhist struct {
	buckets [numAtlasBuckets]uint64
}

func AddHistogram(name string, sampled bool) uint32 {
	idx := atomic.AddUint32(curHistID, 1) - 1

	if idx >= maxNumHists {
		panic("Too many histograms")
	}

	hnames[idx] = name
	hsampled[idx] = sampled
	hists[idx] = newHist()
	bhists[idx] = bhist{}

	return idx
}

func ObserveHist(id uint32, value uint64) {
	h := &hists[id]

	// We lock here to ensure that the min and max values are true to this time
	// period, meaning extractAndReset won't pull the data out from under us
	// while the current observation is being compared. Otherwise, min and max
	// could come from the previous period on the next read. Same with average.
	h.lock.RLock()

	// Keep a running total for average
	atomic.AddUint64(&h.prim.total, value)

	// Set max and min (if needed) in an atomic fashion
	for {
		max := atomic.LoadUint64(&h.prim.max)
		if value < max || atomic.CompareAndSwapUint64(&h.prim.max, max, value) {
			break
		}
	}
	for {
		min := atomic.LoadUint64(&h.prim.min)
		if value > min || atomic.CompareAndSwapUint64(&h.prim.min, min, value) {
			break
		}
	}

	// Record the bucketized histograms
	bucket := getBucket(value)
	atomic.AddUint64(&bhists[id].buckets[bucket], 1)

	// Count and possibly return for sampling
	c := atomic.AddUint64(&h.prim.count, 1)
	if hsampled[id] {
		// Sample, keep every 4th observation
		if (c & 0x3) > 0 {
			h.lock.RUnlock()
			return
		}
	}

	// Get the current index as the count % buflen
	idx := atomic.AddUint64(&h.prim.kept, 1) & buflen

	// Add observation
	h.prim.buf[idx] = value

	// No longer "reading"
	h.lock.RUnlock()
}

func getBucket(n uint64) uint64 {
	if n <= 15 {
		return n
	}

	rshift := 64 - lzcnt(n) - 1
	lshift := rshift

	if lshift&1 == 1 {
		lshift--
	}

	prevPowerOf4 := (n >> rshift) << lshift
	delta := prevPowerOf4 / 3
	offset := int((n - prevPowerOf4) / delta)
	pos := offset + powerOf4Index[lshift/2]

	if pos >= numAtlasBuckets-1 {
		return numAtlasBuckets - 1
	}

	return uint64(pos + 1)
}

func getAllHistograms() map[string]hdat {
	n := int(atomic.LoadUint32(curHistID))

	ret := make(map[string]hdat)

	for i := 0; i < n; i++ {
		ret[hnames[i]] = extractAndReset(&hists[i])
	}

	return ret
}

func extractAndReset(h *hist) hdat {
	h.lock.Lock()

	// flip and reset the count
	h.prim, h.sec = h.sec, h.prim

	atomic.StoreUint64(&h.prim.count, 0)
	atomic.StoreUint64(&h.prim.kept, 0)
	atomic.StoreUint64(&h.prim.total, 0)
	atomic.StoreUint64(&h.prim.max, 0)
	atomic.StoreUint64(&h.prim.min, math.MaxUint64)

	h.lock.Unlock()

	return h.sec
}

func getAllBucketHistograms() map[string][numAtlasBuckets]uint64 {
	n := int(atomic.LoadUint32(curHistID))

	ret := make(map[string][numAtlasBuckets]uint64)

	for i := 0; i < n; i++ {
		ret[hnames[i]] = extractBHist(bhists[i])
	}

	return ret
}

func extractBHist(b bhist) [numAtlasBuckets]uint64 {
	var ret [numAtlasBuckets]uint64
	for i := 0; i < numAtlasBuckets; i++ {
		ret[i] = atomic.LoadUint64(&b.buckets[i])
	}
	return ret
}
