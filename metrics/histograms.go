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
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	maxNumHists            = 1024
	buflen                 = 0x7FFF // max index, 32769 entries
	numAtlasBuckets        = 276
	numIntMetricsPerHist   = 25
	numFloatMetricsPerHist = 1

	tagBucketHistogramPercentile = "percentile"
	bucketHistogramPercentileFmt = "T%04X"
)

var (
	// Buckets and powerOf4Index are based on the generated data from the Spectator library
	// https://github.com/Netflix/spectator/blob/master/spectator-api/src/main/java/com/netflix/spectator/api/histogram/PercentileBuckets.java#L64
	powerOf4Index = [...]int{0, 3, 14, 23, 32, 41, 50, 59, 68, 77, 86, 95, 104, 113, 122, 131, 140, 149, 158, 167, 176, 185, 194, 203, 212, 221, 230, 239, 248, 257, 266, 275}

	bucketValues = [...]int64{
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

	curHistID          = new(uint32)
	hists              = make([]hist, maxNumHists)
	bhists             = make([]bhist, maxNumHists)
	hNames             = make([]string, maxNumHists)
	bhNames            = make([]string, maxNumHists)
	hSampled           = make([]bool, maxNumHists)
	hIntTagsExpanded   = make([]Tags, maxNumHists*numIntMetricsPerHist)
	hFloatTagsExpanded = make([]Tags, maxNumHists*numFloatMetricsPerHist)

	bhTags []Tags
)

func init() {
	atomic.StoreUint32(curHistID, 0)

	bucketTags := Tags{
		TagMetricType: MetricTypeCounter,
		TagDataType:   DataTypeUint64,
	}

	bhTags = make([]Tags, numAtlasBuckets)

	for i := range bhTags {
		t := copyTags(bucketTags)
		t[tagBucketHistogramPercentile] = fmt.Sprintf(bucketHistogramPercentileFmt, i)
		bhTags[i] = t
	}
}

// The hist struct holds a primary and secondary data structure so the reader of
// the histograms will get to read the data out while new observations are made.
// As well, pulling and resetting the histogram does not require a malloc in the
// path of pulling the data, and the large circular buffers can be reused.
type hist struct {
	lock *sync.RWMutex
	dat  hdat
	// backup buffer, switched with the main one on metrics poll
	bakbuf []uint64
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
		lock: &sync.RWMutex{},
		dat: hdat{
			buf: make([]uint64, buflen+1),
			min: math.MaxUint64,
		},
		bakbuf: make([]uint64, buflen+1),
	}
}

type bhist struct {
	buckets [numAtlasBuckets]uint64
}

func copyTags(orig Tags) Tags {
	ret := make(Tags)
	for k, v := range orig {
		ret[k] = v
	}
	return ret
}

// AddHistogram creates a new histogram structure to record observations. The handle
// returned is used to record observations.
// There is a maximum of 1024 histograms, after which adding a new one will panic.
//
// It is recommended to initialize all histograms in a package level var block:
//
//   var (
//       // Create unsampled histogram "foo"
//       HistFoo = metrics.AddHistogram("foo", false, tags{"tag1": "value"})
//   )
//
// Then make observations later:
//
//   start := time.Now().UnixNano()
//   someOperation()
//   end := time.Now().UnixNano() - start
//   metrics.ObserveHist(HistFoo, uint64(end))
func AddHistogram(name string, sampled bool, tgs Tags) uint32 {
	idx := atomic.AddUint32(curHistID, 1) - 1

	if idx >= maxNumHists {
		panic("Too many histograms")
	}

	hists[idx] = newHist()
	bhists[idx] = bhist{}
	hSampled[idx] = sampled

	// There's only ony main metric name per histogram, the rest is tagging
	hNames[idx] = "hist_" + name
	bhNames[idx] = "bhist_" + name

	// This next section precalculates all of the tag sets for all of the metrics for
	// this histogram.

	// guarantee we don't mess with the passed in tags
	tgs = copyTags(tgs)

	// For now the only float metric for each histogram is the average
	t := copyTags(tgs)
	t[TagDataType] = DataTypeFloat64
	t[TagMetricType] = MetricTypeGauge
	t[TagStatistic] = "average"
	hFloatTagsExpanded[idx] = t

	// these same tags can be used for all of the int tags
	tgs[TagDataType] = DataTypeUint64
	tgs[TagMetricType] = MetricTypeCounter

	// The first 21 int metrics in each histogram are the percentiles from 0 to 100 by 5
	for i := uint32(0); i < 21; i++ {
		t = copyTags(tgs)
		t[TagStatistic] = fmt.Sprintf("percentile%d", i*5)

		j := (idx * numIntMetricsPerHist) + i
		hIntTagsExpanded[j] = t
	}

	// Now add the 99th and 99.9th percentiles for 22 and 23
	t = copyTags(tgs)
	t[TagStatistic] = "percentile99"
	hIntTagsExpanded[(idx*numIntMetricsPerHist)+21] = t

	t = copyTags(tgs)
	// a bit brittle. If metricPercentile changes, this should change too
	t[TagStatistic] = "percentile99.9"
	hIntTagsExpanded[(idx*numIntMetricsPerHist)+22] = t

	// Now for the count and kept
	t = copyTags(tgs)
	t[TagStatistic] = "count"
	hIntTagsExpanded[(idx*numIntMetricsPerHist)+23] = t

	t = copyTags(tgs)
	t[TagStatistic] = "kept"
	hIntTagsExpanded[(idx*numIntMetricsPerHist)+24] = t

	return idx
}

// ObserveHist adds an observation to the given histogram. The id parameter is a handle
// returned by the AddHistogram method. Using numbers not returned by AddHistogram is
// undefined behavior and may cause a panic.
func ObserveHist(id uint32, value uint64) {
	h := &hists[id]

	// We lock here to ensure that the min and max values are true to this time
	// period, meaning extractAndReset won't pull the data out from under us
	// while the current observation is being compared. Otherwise, min and max
	// could come from the previous period on the next read. Same with average.
	h.lock.RLock()

	// Keep a running total for average
	atomic.AddUint64(&h.dat.total, value)

	// Set max and min (if needed) in an atomic fashion
	for {
		max := atomic.LoadUint64(&h.dat.max)
		if value < max || atomic.CompareAndSwapUint64(&h.dat.max, max, value) {
			break
		}
	}
	for {
		min := atomic.LoadUint64(&h.dat.min)
		if value > min || atomic.CompareAndSwapUint64(&h.dat.min, min, value) {
			break
		}
	}

	// Record the bucketized histograms
	bucket := getBucket(value)
	atomic.AddUint64(&bhists[id].buckets[bucket], 1)

	// Count and possibly return for sampling
	c := atomic.AddUint64(&h.dat.count, 1)
	if hSampled[id] {
		// Sample, keep every 4th observation
		if (c & 0x3) > 0 {
			h.lock.RUnlock()
			return
		}
	}

	// Get the current index as the count % buflen
	idx := atomic.AddUint64(&h.dat.kept, 1) & buflen

	// Add observation
	h.dat.buf[idx] = value

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

func getAllHistograms() ([]IntMetric, []FloatMetric) {
	// gather all the counters from all the histograms (percentiles, kept, count)
	//   into the intmetrics
	// gather all the averages into floatmetrics

	n := int(atomic.LoadUint32(curHistID))

	// 23 percentile metrics + kept and count per histogram
	intret := make([]IntMetric, 0, n*numIntMetricsPerHist)
	// 1 float metric per histogram, the average
	floatret := make([]FloatMetric, 0, n)

	for i := 0; i < n; i++ {
		name := hNames[i]
		dat := extractHist(&hists[i])

		// If there's no observations, skip printing this histogram.
		// Printing out 0 will give bad data on graphs. We're better having no
		// data than a zero.
		// We do always want the count at least, so there is a metric showing that
		// there shouldn't be any other metrics for this hist.
		if dat.count == 0 {
			idx := i*numIntMetricsPerHist + 23
			intret = append(intret, IntMetric{
				Name: name,
				Val:  dat.count,
				Tgs:  hIntTagsExpanded[idx],
			})
			continue
		}

		avg := float64(dat.total) / float64(dat.count)
		floatret = append(floatret, FloatMetric{
			Name: name,
			Val:  avg,
			Tgs:  hFloatTagsExpanded[i],
		})

		// The percentiles returned from hdatPercentiles and the tags set up
		// in the AddHistogram function should all match
		pctls := hdatPercentiles(dat)
		for j := 0; j < 23; j++ {
			idx := i*numIntMetricsPerHist + j
			intret = append(intret, IntMetric{
				Name: name,
				Val:  pctls[j],
				Tgs:  hIntTagsExpanded[idx],
			})
		}

		// now for count and kept
		idx := i*numIntMetricsPerHist + 23
		intret = append(intret, IntMetric{
			Name: name,
			Val:  dat.count,
			Tgs:  hIntTagsExpanded[idx],
		})

		idx++
		intret = append(intret, IntMetric{
			Name: name,
			Val:  dat.kept,
			Tgs:  hIntTagsExpanded[idx],
		})
	}

	return intret, floatret
}

func extractHist(h *hist) hdat {
	h.lock.Lock()

	// move primary to the side for reporting, keeping a hold of the buffer.
	// Primary gets reset but keeps the old buffer to avoid reallocating in the
	// critical section while observation recording is blocked.
	ret := h.dat

	// new hdat with old buf
	h.dat = hdat{
		buf: h.bakbuf,
		min: math.MaxUint64,
	}

	// keep a hold of the buf
	h.bakbuf = ret.buf

	h.lock.Unlock()

	return ret
}

func getAllBucketHistograms() []IntMetric {
	n := int(atomic.LoadUint32(curHistID))

	var ret = make([]IntMetric, 0, n*numAtlasBuckets)

	// For each histogram, loop over all of the values and add them as separate intmetric
	// values to the return value
	for i := 0; i < n; i++ {
		for j, val := range extractBHist(bhists[i]) {
			ret = append(ret, IntMetric{bhNames[i], val, bhTags[j]})
		}
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

// Percentiles go by 5% percentile steps from min to max. We report all of them even though it's
// likely only min, 25th, 50th, 75th, 95th, 99th, and max will be used. It's assumed the metric
// poller that is consuming this output will choose to only report to the metrics system what it
// considers useful information.
//
// Slice layout:
//  [0]: min (0th)
//  [1]: 5th
//  [n]: 5n
//  [19]: 95th
//  [20]: max (100th)
//  [21]: 99th
//  [22]: 99.9th
func hdatPercentiles(dat hdat) [23]uint64 {
	buf := dat.buf
	kept := dat.kept

	var pctls [23]uint64

	if kept == 0 {
		return pctls
	}
	if kept < uint64(len(buf)) {
		buf = buf[:kept]
	}

	sort.Sort(uint64slice(buf))

	// Take care of 0th and 100th specially
	pctls[0] = dat.min
	pctls[20] = dat.max

	// 5th - 95th
	for i := 1; i < 20; i++ {
		idx := len(buf) * i / 20
		pctls[i] = buf[idx]
	}

	// Add 99th and 99.9th
	idx := len(buf) * 99 / 100
	pctls[21] = buf[idx]

	idx = int(math.Floor(float64(len(buf)) * 99.9 / 100.0))
	pctls[22] = buf[idx]

	return pctls
}
