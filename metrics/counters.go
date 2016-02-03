package metrics

import (
	"sync"
	"sync/atomic"
)

var (
	names    = make([]string, 0)
	counters = make([]*uint64, 0)
	lock     = new(sync.RWMutex)
)

// Registers a counter and returns an ID that can be used to access it
func AddCounter(name string) int {
	lock.Lock()
	defer lock.Unlock()

	counterID := len(names)

	names = append(names, name)
	counters = append(counters, new(uint64))

	return counterID
}

func IncCounter(id int) {
	lock.RLock()
	defer lock.RUnlock()

	atomic.AddUint64(counters[id], 1)
}

func IncCounterBy(id int, amount uint64) {
	lock.RLock()
	defer lock.RUnlock()

	atomic.AddUint64(counters[id], amount)
}

func GetAllCounters() map[string]uint64 {
	lock.RLock()
	defer lock.RUnlock()

	ret := make(map[string]uint64)

	for i, name := range names {
		ret[name] = atomic.LoadUint64(counters[i])
	}

	return ret
}
