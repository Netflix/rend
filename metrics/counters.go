package metrics

import (
	"sync"
	"sync/atomic"
)

var (
	counters = make(map[string]*uint64)
	lock     = new(sync.RWMutex)
)

func AddCounter(name string) {
	lock.Lock()
	defer lock.Unlock()

	temp := new(uint64)
	atomic.StoreUint64(temp, 0)
	counters[name] = temp
}

func IncCounter(name string) {
	lock.RLock()
	defer lock.RUnlock()

	atomic.AddUint64(counters[name], 1)
}

func IncCounterBy(name string, amount uint64) {
	lock.RLock()
	defer lock.RUnlock()

	atomic.AddUint64(counters[name], amount)
}

func GetCounter(name string) uint64 {
	lock.RLock()
	defer lock.RUnlock()

	return atomic.LoadUint64(counters[name])
}

func GetAllCounters() map[string]uint64 {
	lock.RLock()
	defer lock.RUnlock()

	ret := make(map[string]uint64)
	for name, val := range counters {
		ret[name] = atomic.LoadUint64(val)
	}
	return ret
}
