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
	"net/http"
	"runtime"
	"sort"
)

var prefix = ""

func SetPrefix(p string) {
	prefix = p
}

var memstats = new(runtime.MemStats)

func init() {
	http.Handle("/metrics", http.HandlerFunc(printMetrics))
}

func printMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	// Runtime memory stats
	runtime.ReadMemStats(memstats)
	// General statistics.
	fmt.Fprintf(w, "%smem_alloc %d\n", prefix, memstats.Alloc)            // bytes allocated and not yet freed
	fmt.Fprintf(w, "%smem_alloc_total %d\n", prefix, memstats.TotalAlloc) // bytes allocated (even if freed)
	fmt.Fprintf(w, "%smem_sys %d\n", prefix, memstats.Sys)                // bytes obtained from system (sum of XxxSys below)
	fmt.Fprintf(w, "%smem_ptr_lookups %d\n", prefix, memstats.Lookups)    // number of pointer lookups
	fmt.Fprintf(w, "%smem_mallocs %d\n", prefix, memstats.Mallocs)        // number of mallocs
	fmt.Fprintf(w, "%smem_frees %d\n", prefix, memstats.Frees)            // number of frees

	// Main allocation heap statistics.
	fmt.Fprintf(w, "%smem_heap_alloc %d\n", prefix, memstats.HeapAlloc)       // bytes allocated and not yet freed (same as Alloc above)
	fmt.Fprintf(w, "%smem_heap_sys %d\n", prefix, memstats.HeapSys)           // bytes obtained from system
	fmt.Fprintf(w, "%smem_heap_idle %d\n", prefix, memstats.HeapIdle)         // bytes in idle spans
	fmt.Fprintf(w, "%smem_heap_in_use %d\n", prefix, memstats.HeapInuse)      // bytes in non-idle span
	fmt.Fprintf(w, "%smem_heap_released %d\n", prefix, memstats.HeapReleased) // bytes released to the OS
	fmt.Fprintf(w, "%smem_heap_objects %d\n", prefix, memstats.HeapObjects)   // total number of allocated objects

	fmt.Fprintf(w, "%smem_stack_in_use %d\n", prefix, memstats.StackInuse) // bytes used by stack allocator
	fmt.Fprintf(w, "%smem_stack_sys %d\n", prefix, memstats.StackSys)
	fmt.Fprintf(w, "%smem_mspan_in_use %d\n", prefix, memstats.MSpanInuse) // mspan structures
	fmt.Fprintf(w, "%smem_mspan_sys %d\n", prefix, memstats.MSpanSys)
	fmt.Fprintf(w, "%smem_mcache_in_use %d\n", prefix, memstats.MCacheInuse) // mcache structures
	fmt.Fprintf(w, "%smem_mcache_sys %d\n", prefix, memstats.MCacheSys)
	fmt.Fprintf(w, "%smem_buck_hash_sys %d\n", prefix, memstats.BuckHashSys) // profiling bucket hash table
	fmt.Fprintf(w, "%smem_gc_sys %d\n", prefix, memstats.GCSys)              // GC metadata
	fmt.Fprintf(w, "%smem_other_sys %d\n", prefix, memstats.OtherSys)        // other system allocations

	fmt.Fprintf(w, "%sgc_next_gc_heap_alloc %d\n", prefix, memstats.NextGC) // next collection will happen when HeapAlloc ≥ this amount
	fmt.Fprintf(w, "%sgc_last_gc_time %d\n", prefix, memstats.LastGC)       // end time of last collection (nanoseconds since 1970)
	fmt.Fprintf(w, "%sgc_pause_total %d\n", prefix, memstats.PauseTotalNs)
	fmt.Fprintf(w, "%sgc_num_gc %d\n", prefix, memstats.NumGC)
	fmt.Fprintf(w, "%sgc_gc_cpu_frac %f\n", prefix, memstats.GCCPUFraction)

	// circular buffer of recent GC pause durations, most recent at [(NumGC+255)%256]
	pctls := pausePercentiles(memstats.PauseNs[:], memstats.NumGC)
	for i := 0; i < 20; i++ {
		p := pctls[i]
		fmt.Fprintf(w, "%sgc_pause_pctl_%d %d\n", prefix, i*5, p)
	}
	fmt.Fprintf(w, "%sgc_pause_pctl_%d %d\n", prefix, 99, pctls[20])
	fmt.Fprintf(w, "%sgc_pause_pctl_%d %d\n", prefix, 100, pctls[21])

	// Per-size allocation statistics.
	for _, b := range memstats.BySize {
		fmt.Fprintf(w, "%salloc_size_%d_mallocs %d\n", prefix, b.Size, b.Mallocs)
		fmt.Fprintf(w, "%salloc_size_%d_frees %d\n", prefix, b.Size, b.Frees)
	}

	// Histograms
	hists := getAllHistograms()
	for name, dat := range hists {
		fmt.Fprintf(w, "%shist_%s_count %d\n", prefix, name, dat.count)
		fmt.Fprintf(w, "%shist_%s_kept %d\n", prefix, name, dat.kept)

		if dat.total > 0 && dat.count > 0 {
			avg := float64(dat.total) / float64(dat.count)
			fmt.Fprintf(w, "%shist_%s_avg %f\n", prefix, name, avg)
		}

		pctls := hdatPercentiles(dat)
		if len(pctls) == 0 {
			continue
		}
		for i := 0; i < 20; i++ {
			p := pctls[i]
			fmt.Fprintf(w, "%shist_%s_pctl_%d %d\n", prefix, name, i*5, p)
		}
		fmt.Fprintf(w, "%shist_%s_pctl_%d %d\n", prefix, name, 99, pctls[20])
		fmt.Fprintf(w, "%shist_%s_pctl_%d %d\n", prefix, name, 100, pctls[21])
	}

	// Bucketized histograms
	// Buckets are based on the number of leading zeros in the number
	// so each is less than or equal to that number of ones. Therefore
	// bucket 0 means the number was greater than 0x7FFF_FFFF_FFFF_FFFF
	// and less than or equal to 0xFFFF_FFFF_FFFF_FFFF, which would be a huge
	// duration but it's a good example. As well, bucket 63 only hold values
	// 0x0 and 0x1. Bucket 62 hold 0x10 and 0x11. 61: 0x100, 0x101, 0x110, 0x111
	bhists := getAllBucketHistograms()
	for name, bh := range bhists {
		var bmax uint64 = math.MaxUint64 // 0xFFFF_FFFF_FFFF_FFFF
		for i := 0; i < 64; i++ {
			fmt.Fprintf(w, "%sbhist_%s_bucket_%d %d\n", prefix, name, bmax, bh[i])
			bmax >>= 1
		}
	}

	// Application stats
	ctrs := getAllCounters()
	for name, val := range ctrs {
		fmt.Fprintf(w, "%s%s %d\n", prefix, name, val)
	}
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
//  [20]: 99th
//  [21]: max (100th)
func hdatPercentiles(dat *hdat) []uint64 {
	buf := dat.buf
	kept := dat.kept

	if kept == 0 {
		return nil
	}
	if kept < uint64(len(buf)) {
		buf = buf[:kept]
	}

	sort.Sort(uint64slice(buf))

	pctls := make([]uint64, 22)

	// Take care of 0th and 100th specially
	pctls[0] = dat.min
	pctls[21] = dat.max

	// 5th - 95th
	for i := 1; i < 20; i++ {
		idx := len(buf) * i / 20
		pctls[i] = buf[idx]
	}

	// Add 99th
	idx := len(buf) * 99 / 100
	pctls[20] = buf[idx]

	return pctls
}

func pausePercentiles(pauses []uint64, ngc uint32) []uint64 {
	if ngc < uint32(len(pauses)) {
		pauses = pauses[:ngc]
	}

	sort.Sort(uint64slice(pauses))

	pctls := make([]uint64, 22)

	// Take care of 0th and 100th specially
	pctls[0] = pauses[0]
	pctls[21] = pauses[len(pauses)-1]

	// 5th - 95th
	for i := 1; i < 20; i++ {
		idx := len(pauses) * i / 20
		pctls[i] = pauses[idx]
	}

	// Add 99th
	idx := len(pauses) * 99 / 100
	pctls[20] = pauses[idx]

	return pctls
}

type uint64slice []uint64

func (u uint64slice) Len() int           { return len(u) }
func (u uint64slice) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u uint64slice) Less(i, j int) bool { return u[i] < u[j] }
