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

import "sync/atomic"

type IntGaugeCallback func() uint64
type FloatGaugeCallback func() float64

const maxNumCallbacks = 10240

var (
	intcbnames     = make([]string, maxNumCallbacks)
	floatcbnames   = make([]string, maxNumCallbacks)
	intcallbacks   = make([]IntGaugeCallback, maxNumCallbacks)
	floatcallbacks = make([]FloatGaugeCallback, maxNumCallbacks)
	curIntCbID     = new(uint32)
	curFloatCbID   = new(uint32)
)

// Registers a gauge callback which will be called every time metrics are requested.
// There is a maximum of 1024 callbacks, after which adding a new one will panic
func RegisterIntGaugeCallback(name string, cb IntGaugeCallback) {
	id := atomic.AddUint32(curIntCbID, 1) - 1

	if id >= maxNumCallbacks {
		panic("Too many callbacks")
	}

	intcallbacks[id] = cb
	intcbnames[id] = name
}

// Registers a gauge callback which will be called every time metrics are requested.
// There is a maximum of 1024 callbacks, after which adding a new one will panic
func RegisterFloatGaugeCallback(name string, cb FloatGaugeCallback) {
	id := atomic.AddUint32(curFloatCbID, 1) - 1

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
