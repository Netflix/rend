package metrics

import "sync/atomic"

const maxNumGauges = 1024

var (
	gnames     = make([]string, maxNumGauges)
	gauges     = make([]uint64, maxNumGauges)
	curGaugeID = new(uint32)
)

func init() {
	// start with "-1" so the first metric ID overflows to 0
	atomic.StoreUint32(curGaugeID, 0xFFFFFFFF)
}

// Registers a gauge and returns an ID that can be used to access it
// There is a maximum of 1024 gauges, after which adding a new one will panic
func AddGauge(name string) uint32 {
	id := atomic.AddUint32(curGaugeID, 1)

	if id >= maxNumGauges {
		panic("Too many gauges")
	}

	gnames[id] = name
	return id
}

func SetGauge(id uint32, value uint64) {
	atomic.StoreUint64(&gauges[id], value)
}

func getAllGauges() map[string]uint64 {
	ret := make(map[string]uint64)
	numIDs := int(atomic.LoadUint32(curGaugeID))

	for i := 0; i < numIDs; i++ {
		ret[gnames[i]] = atomic.LoadUint64(&gauges[i])
	}

	return ret
}
