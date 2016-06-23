package metrics

import "sync/atomic"

type IntGaugeCallback func() uint64
type FloatGaugeCallback func() float64

const maxNumCallbacks = 1024

var (
	intcbnames     = make([]string, maxNumCallbacks)
	floatcbnames   = make([]string, maxNumCallbacks)
	intcallbacks   = make([]IntGaugeCallback, maxNumCallbacks)
	floatcallbacks = make([]FloatGaugeCallback, maxNumCallbacks)
	curIntCbID     = new(uint32)
	curFloatCbID   = new(uint32)
)

func init() {
	// start with "-1" so the first metric ID overflows to 0
	atomic.StoreUint32(curIntCbID, 0xFFFFFFFF)
	atomic.StoreUint32(curFloatCbID, 0xFFFFFFFF)
}

// Registers a gauge callback which will be called every time metrics are requested.
// There is a maximum of 1024 callbacks, after which adding a new one will panic
func RegisterIntGaugeCallback(name string, cb IntGaugeCallback) {
	id := atomic.AddUint32(curIntCbID, 1)

	if id >= maxNumCallbacks {
		panic("Too many callbacks")
	}

	intcallbacks[id] = cb
	intcbnames[id] = name
}

// Registers a gauge callback which will be called every time metrics are requested.
// There is a maximum of 1024 callbacks, after which adding a new one will panic
func RegisterFloatGaugeCallback(name string, cb FloatGaugeCallback) {
	id := atomic.AddUint32(curFloatCbID, 1)

	if id >= maxNumCallbacks {
		panic("Too many callbacks")
	}

	floatcallbacks[id] = cb
	floatcbnames[id] = name
}

func getAllCallbackGauges() (map[string]uint64, map[string]float64) {
	retint := make(map[string]uint64)
	numIDs := int(atomic.LoadUint32(curIntCbID))

	for i := 0; i < numIDs; i++ {
		retint[intcbnames[i]] = intcallbacks[i]()
	}

	retfloat := make(map[string]float64)
	numIDs = int(atomic.LoadUint32(curFloatCbID))

	for i := 0; i < numIDs; i++ {
		retfloat[floatcbnames[i]] = floatcallbacks[i]()
	}

	return retint, retfloat
}
