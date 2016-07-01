package metrics

import (
	"sync/atomic"
	"unsafe"
)

const maxNumGauges = 1024

var (
	intgnames       = make([]string, maxNumGauges)
	floatgnames     = make([]string, maxNumGauges)
	intgauges       = make([]uint64, maxNumGauges)
	floatgauges     = make([]uint64, maxNumGauges)
	curIntGaugeID   = new(uint32)
	curFloatGaugeID = new(uint32)
)

func init() {
	atomic.StoreUint32(curIntGaugeID, 0)
	atomic.StoreUint32(curFloatGaugeID, 0)
}

// Registers a gauge and returns an ID that can be used to access it
// There is a maximum of 1024 gauges, after which adding a new one will panic
func AddIntGauge(name string) uint32 {
	id := atomic.AddUint32(curIntGaugeID, 1) - 1

	if id >= maxNumGauges {
		panic("Too many gauges")
	}

	intgnames[id] = name
	return id
}

// Registers a gauge and returns an ID that can be used to access it
// There is a maximum of 1024 gauges, after which adding a new one will panic
func AddFloatGauge(name string) uint32 {
	id := atomic.AddUint32(curFloatGaugeID, 1) - 1

	if id >= maxNumGauges {
		panic("Too many gauges")
	}

	floatgnames[id] = name
	return id
}

func SetIntGauge(id uint32, value uint64) {
	atomic.StoreUint64(&intgauges[id], value)
}

func SetFloatGauge(id uint32, value float64) {
	// The float64 value needs to be converted into an int64 here because
	// there is no atomic store for float values. This is a literal
	// reinterpretation of the same exact bits.
	v2 := *(*uint64)(unsafe.Pointer(&value))
	atomic.StoreUint64(&floatgauges[id], v2)
}

func getAllGauges() (map[string]uint64, map[string]float64) {
	retint := make(map[string]uint64)
	numIDs := int(atomic.LoadUint32(curIntGaugeID))

	for i := 0; i < numIDs; i++ {
		retint[intgnames[i]] = atomic.LoadUint64(&intgauges[i])
	}

	retfloat := make(map[string]float64)
	numIDs = int(atomic.LoadUint32(curFloatGaugeID))

	for i := 0; i < numIDs; i++ {
		// The int64 bit pattern of the float value needs to be converted back
		// into a float64 here. This is a literal reinterpretation of the same
		// exact bits.
		intval := atomic.LoadUint64(&floatgauges[i])
		floatval := *(*float64)(unsafe.Pointer(&intval))
		retfloat[floatgnames[i]] = floatval
	}

	return retint, retfloat
}
