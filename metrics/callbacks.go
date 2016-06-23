package metrics

import "sync/atomic"

type GaugeCallback func() uint64

const maxNumCallbacks = 1024

var (
	cbnames   = make([]string, maxNumCallbacks)
	callbacks = make([]GaugeCallback, maxNumCallbacks)
	curCbID   = new(uint32)
)

func init() {
	// start with "-1" so the first metric ID overflows to 0
	atomic.StoreUint32(curCbID, 0xFFFFFFFF)
}

// Registers a gauge callback which will be called every time metrics are requested.
// There is a maximum of 1024 callbacks, after which adding a new one will panic
func RegisterGaugeCallback(name string, cb GaugeCallback) {
	id := atomic.AddUint32(curCbID, 1)

	if id >= maxNumCallbacks {
		panic("Too many callbacks")
	}

	callbacks[id] = cb
	cbnames[id] = name
}

func getAllCallbackGauges() map[string]uint64 {
	ret := make(map[string]uint64)
	numIDs := int(atomic.LoadUint32(curCbID))

	for i := 0; i < numIDs; i++ {
		ret[cbnames[i]] = callbacks[i]()
	}

	return ret
}
