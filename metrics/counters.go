package metrics

import "sync/atomic"

const numMetrics = 1024

var (
	cnames       = make([]string, numMetrics)
	counters     = make([]*uint64, numMetrics)
	curCounterID = new(uint32)
)

func init() {
	// start with "-1" so the first metric ID overflows to 0
	atomic.StoreUint32(curCounterID, 0xFFFFFFFF)
}

// Registers a counter and returns an ID that can be used to access it
// There is a maximum of 1024 metrics, after which adding a new one will panic
func AddCounter(name string) uint32 {
	id := atomic.AddUint32(curCounterID, 1)
	cnames[id] = name
	counters[id] = new(uint64)
	return id
}

func IncCounter(id uint32) {
	atomic.AddUint64(counters[id], 1)
}

func IncCounterBy(id uint32, amount uint64) {
	atomic.AddUint64(counters[id], amount)
}

func getAllCounters() map[string]uint64 {
	ret := make(map[string]uint64)
	numIDs := int(atomic.LoadUint32(curCounterID))

	for i := 0; i < numIDs; i++ {
		ret[cnames[i]] = atomic.LoadUint64(counters[i])
	}

	return ret
}
