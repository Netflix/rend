package metrics

import "sync/atomic"

const numMetrics = 1024

var (
	names    = make([]string, numMetrics)
	counters = make([]*uint64, numMetrics)
	curID    = new(uint32)
)

func init() {
	// start with "-1" so the first metric ID overflows to 0
	atomic.StoreUint32(curID, 0xFFFFFFFF)
}

// Registers a counter and returns an ID that can be used to access it
// There is a maximum of 1024 metrics, after which adding a new one will panic
func AddCounter(name string) uint32 {
	id := atomic.AddUint32(curID, 1)
	names[id] = name
	counters[id] = new(uint64)
	return id
}

func IncCounter(id uint32) {
	atomic.AddUint64(counters[id], 1)
}

func IncCounterBy(id uint32, amount uint64) {
	atomic.AddUint64(counters[id], amount)
}

func GetAllCounters() map[string]uint64 {
	ret := make(map[string]uint64)
	numIDs := int(atomic.LoadUint32(curID))

	for i := 0; i < numIDs; i++ {
		ret[names[i]] = atomic.LoadUint64(counters[i])
	}

	return ret
}
