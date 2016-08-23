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
	"io"
	"net/http"
	"runtime"
	"sort"
	"sync"
)

var prefix = ""

func SetPrefix(p string) {
	prefix = p
}

var (
	memstats        = new(runtime.MemStats)
	metricsReadLock = new(sync.Mutex)

	tagsIntCounter = Tags{
		TagMetricType: MetricTypeCounter,
		TagDataType:   DataTypeUint64,
	}
	tagsIntGauge = Tags{
		TagMetricType: MetricTypeGauge,
		TagDataType:   DataTypeUint64,
	}
	tagsFloatGauge = Tags{
		TagMetricType: MetricTypeGauge,
		TagDataType:   DataTypeUint64,
	}

	percentileTags [22]Tags
	allocTags      []Tags
)

func init() {
	http.Handle("/metrics", http.HandlerFunc(printMetrics))

	// pre-calculate tags for percentile metrics
	for i := 0; i < 21; i++ {
		t := copyTags(tagsFloatGauge)
		t[TagStatistic] = fmt.Sprintf("percentile%d", i*5)
		percentileTags[i] = t
	}

	t := copyTags(tagsFloatGauge)
	t[TagStatistic] = "percentile99"
	percentileTags[21] = t

	// pre-calculate tags for memory allocation statistics
	ms := new(runtime.MemStats)
	runtime.ReadMemStats(ms)
	for _, s := range ms.BySize {
		t := copyTags(tagsIntCounter)
		t["size"] = fmt.Sprintf("%d", s.Size)
		allocTags = append(allocTags, t)
	}
}

func printMetrics(w http.ResponseWriter, r *http.Request) {
	// prevent concurrent access to metrics. This is an assumption helpd by much of
	// the code that retrieves the metrics for printing.
	metricsReadLock.Lock()
	defer metricsReadLock.Unlock()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	//////////////////////////
	// Runtime memory stats
	//////////////////////////
	runtime.ReadMemStats(memstats)

	var im []IntMetric
	var fm []FloatMetric

	// General statistics.
	im = append(im, IntMetric{"mem_alloc", memstats.Alloc, tagsIntGauge})            // bytes allocated and not yet freed
	im = append(im, IntMetric{"mem_alloc_total", memstats.TotalAlloc, tagsIntGauge}) // bytes allocated (even if freed)
	im = append(im, IntMetric{"mem_sys", memstats.Sys, tagsIntGauge})                // bytes obtained from system (sum of XxxSys below)
	im = append(im, IntMetric{"mem_ptr_lookups", memstats.Lookups, tagsIntCounter})  // number of pointer lookups
	im = append(im, IntMetric{"mem_mallocs", memstats.Mallocs, tagsIntCounter})      // number of mallocs
	im = append(im, IntMetric{"mem_frees", memstats.Frees, tagsIntCounter})          // number of frees

	// Main allocation heap statistics.
	im = append(im, IntMetric{"mem_heap_alloc", memstats.HeapAlloc, tagsIntGauge})       // bytes allocated and not yet freed (same as Alloc above)
	im = append(im, IntMetric{"mem_heap_sys", memstats.HeapSys, tagsIntGauge})           // bytes obtained from system
	im = append(im, IntMetric{"mem_heap_idle", memstats.HeapIdle, tagsIntGauge})         // bytes in idle spans
	im = append(im, IntMetric{"mem_heap_in_use", memstats.HeapInuse, tagsIntGauge})      // bytes in non-idle span
	im = append(im, IntMetric{"mem_heap_released", memstats.HeapReleased, tagsIntGauge}) // bytes released to the OS
	im = append(im, IntMetric{"mem_heap_objects", memstats.HeapObjects, tagsIntGauge})   // total number of allocated objects

	// Secondary detailed heap stats
	im = append(im, IntMetric{"mem_stack_in_use", memstats.StackInuse, tagsIntGauge}) // bytes used by stack allocator
	im = append(im, IntMetric{"mem_stack_sys", memstats.StackSys, tagsIntGauge})
	im = append(im, IntMetric{"mem_mspan_in_use", memstats.MSpanInuse, tagsIntGauge}) // mspan structures
	im = append(im, IntMetric{"mem_mspan_sys", memstats.MSpanSys, tagsIntGauge})
	im = append(im, IntMetric{"mem_mcache_in_use", memstats.MCacheInuse, tagsIntGauge}) // mcache structures
	im = append(im, IntMetric{"mem_mcache_sys", memstats.MCacheSys, tagsIntGauge})
	im = append(im, IntMetric{"mem_buck_hash_sys", memstats.BuckHashSys, tagsIntGauge}) // profiling bucket hash table
	im = append(im, IntMetric{"mem_gc_sys", memstats.GCSys, tagsIntGauge})              // GC metadata
	im = append(im, IntMetric{"mem_other_sys", memstats.OtherSys, tagsIntGauge})        // other system allocations

	im = append(im, IntMetric{"gc_next_gc_heap_alloc", memstats.NextGC, tagsIntGauge}) // next collection will happen when HeapAlloc ≥ this amount
	im = append(im, IntMetric{"gc_last_gc_time", memstats.LastGC, tagsIntGauge})       // end time of last collection (nanoseconds since 1970)
	im = append(im, IntMetric{"gc_pause_total", memstats.PauseTotalNs, tagsIntCounter})
	im = append(im, IntMetric{"gc_num_gc", uint64(memstats.NumGC), tagsIntCounter})

	fm = append(fm, FloatMetric{"gc_gc_cpu_frac", memstats.GCCPUFraction, tagsFloatGauge})

	// circular buffer of recent GC pause durations, most recent at [(NumGC+255)%256]
	pctls := pausePercentiles(memstats.PauseNs[:], memstats.NumGC)
	for i := 0; i < 22; i++ {
		// pre-calculated tags match the 0:5:100,99 pattern that pausePercentiles produces.
		im = append(im, IntMetric{"gc_pause", pctls[i], percentileTags[i]})
	}

	// Per-size allocation statistics.
	for i, b := range memstats.BySize {
		im = append(im, IntMetric{"alloc_mallocs", b.Mallocs, allocTags[i]})
		im = append(im, IntMetric{"alloc_frees", b.Frees, allocTags[i]})
	}

	//////////////////////////
	// Histograms
	//////////////////////////
	inth, floath := getAllHistograms()
	im = append(im, inth...)
	fm = append(fm, floath...)

	//////////////////////////
	// Bucketized histograms
	//////////////////////////
	im = append(im, getAllBucketHistograms()...)

	//////////////////////////
	// Counters
	//////////////////////////
	im = append(im, getAllCounters()...)

	//////////////////////////
	// Gauges
	//////////////////////////
	intg, floatg := getAllGauges()
	im = append(im, intg...)
	fm = append(fm, floatg...)

	//////////////////////////
	// Gauge Callbacks
	//////////////////////////
	intg, floatg = getAllCallbackGauges()
	im = append(im, intg...)
	fm = append(fm, floatg...)

	printIntMetrics(w, im)
	printFloatMetrics(w, fm)
}

func makeTags(typ, dataType, statistic string) Tags {
	ret := Tags{
		TagMetricType: typ,
		TagDataType:   dataType,
	}

	if statistic != "" {
		ret[TagStatistic] = statistic
	}

	return ret
}

func printTags(tags Tags) string {
	var ret []byte

	for k, v := range tags {
		ret = append(ret, byte('|'))
		ret = append(ret, k...)
		ret = append(ret, byte('*'))
		ret = append(ret, v...)
	}

	return string(ret)
}

func printIntMetrics(w io.Writer, metrics []IntMetric) {
	for _, m := range metrics {
		fmt.Fprintf(w, "%s%s%s %d\n", prefix, m.Name, printTags(m.Tgs), m.Val)
	}
}

func printFloatMetrics(w io.Writer, metrics []FloatMetric) {
	for _, m := range metrics {
		fmt.Fprintf(w, "%s%s%s %f\n", prefix, m.Name, printTags(m.Tgs), m.Val)
	}
}

// the first 21 positions are the percentiles by 5's from 0 to 100
// the 22nd position is the bonus 99th percentile
// this makes looping over the values easier
func pausePercentiles(pauses []uint64, ngc uint32) []uint64 {
	if ngc < uint32(len(pauses)) {
		pauses = pauses[:ngc]
	}

	sort.Sort(uint64slice(pauses))

	pctls := make([]uint64, 22)

	// Take care of 0th and 100th specially
	pctls[0] = pauses[0]
	pctls[20] = pauses[len(pauses)-1]

	// 5th - 95th
	for i := 1; i < 20; i++ {
		idx := len(pauses) * i / 20
		pctls[i] = pauses[idx]
	}

	// Add 99th
	idx := len(pauses) * 99 / 100
	pctls[21] = pauses[idx]

	return pctls
}

type uint64slice []uint64

func (u uint64slice) Len() int           { return len(u) }
func (u uint64slice) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u uint64slice) Less(i, j int) bool { return u[i] < u[j] }
