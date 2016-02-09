package metrics

import (
	"fmt"
	"net/http"
	"runtime"
	"runtime/debug"
	"sort"
	"time"
)

const prefix = "rend_"

var (
	memstats = new(runtime.MemStats)
	gcstats  = &debug.GCStats{
		PauseQuantiles: make([]time.Duration, 21),
	}
)

func init() {
	http.Handle("/metrics/counters", http.HandlerFunc(printCounters))
}

func printCounters(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	/*/ Runtime memory stats
	runtime.ReadMemStats(memstats)
	fmt.Fprintf(w, "%smem_", prefix, memstats)
	fmt.Fprintf(w, "%smem_", prefix, memstats)
	fmt.Fprintf(w, "%smem_", prefix, memstats)
	fmt.Fprintf(w, "%smem_", prefix, memstats)
	fmt.Fprintf(w, "%smem_", prefix, memstats)
	fmt.Fprintf(w, "%smem_", prefix, memstats)
	fmt.Fprintf(w, "%smem_", prefix, memstats)
	fmt.Fprintf(w, "%smem_", prefix, memstats)
	fmt.Fprintf(w, "%smem_", prefix, memstats)
	fmt.Fprintf(w, "%smem_", prefix, memstats)
	fmt.Fprintf(w, "%smem_", prefix, memstats)

	fmt.Fprintf(w, "%sgc_", prefix, memstats)*/

	// Histograms
	hists := getAllHistograms()
	for name, dat := range hists {
		for i, p := range gatherPercentiles(dat) {
			fmt.Fprintf(w, "%shist_%s_pctl_%d %d\n", prefix, name, i*5, p)
		}
	}

	// Application stats
	ctrs := getAllCounters()
	for name, val := range ctrs {
		fmt.Fprintf(w, "%s%s %d\n", prefix, name, val)
	}
}

// Percentiles go by 5% percentile steps from min to max. We report all of them even though it's
// likely only min, 25th, 50th, 75th, 95th, and max will be used. It's assumed the metric poller
// that is consuming this output will choose to only report to the metrics system what it considers
// useful information.
//
// Slice layout:
//  [0]: min (0th)
//  [1]: 5th
//  [n]: 5n
//  [19]: 95th
//  [20]: max (100th)
func gatherPercentiles(dat *hdat) []uint64 {
	buf := dat.buf
	kept := *dat.kept

	if kept == 0 {
		return nil
	}
	if kept < uint64(len(buf)) {
		buf = buf[:kept]
	}

	sort.Sort(uint64slice(buf))

	pctls := make([]uint64, 21)
	pctls[0] = *dat.min
	pctls[20] = *dat.max

	for i := uint64(1); i < 20; i++ {
		idx := kept * i / 20
		pctls[i] = buf[idx]
	}

	return pctls
}

type uint64slice []uint64

func (u uint64slice) Len() int           { return len(u) }
func (u uint64slice) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u uint64slice) Less(i, j int) bool { return u[i] < u[j] }

type durationslice []time.Duration

func (d durationslice) Len() int           { return len(d) }
func (d durationslice) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d durationslice) Less(i, j int) bool { return d[i] < d[j] }
