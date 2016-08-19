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

// IntGaugeCallback defines a function that the metrics package can use to
// retrieve an integer gauge value
type IntGaugeCallback func() uint64

// FloatGaugeCallback defines a function that the metrics package can use to
// retrieve an floating point gauge value
type FloatGaugeCallback func() float64

const maxNumCallbacks = 10240

var (
	curIntCbID   = new(uint32)
	intcbnames   = make([]string, maxNumCallbacks)
	intcallbacks = make([]IntGaugeCallback, maxNumCallbacks)
	intcbtags    = make([]map[string]string, maxNumCallbacks)

	curFloatCbID   = new(uint32)
	floatcbnames   = make([]string, maxNumCallbacks)
	floatcallbacks = make([]FloatGaugeCallback, maxNumCallbacks)
	floatcbtags    = make([]map[string]string, maxNumCallbacks)
)

// RegisterIntGaugeCallback registers a gauge callback which will be called every
// time metrics are requested.
// There is a maximum of 10240 int callbacks, after which adding a new one will panic.
func RegisterIntGaugeCallback(name string, tags map[string]string, cb IntGaugeCallback) {
	id := atomic.AddUint32(curIntCbID, 1) - 1

	if id >= maxNumCallbacks {
		panic("Too many callbacks")
	}

	intcallbacks[id] = cb
	intcbnames[id] = name

	tags[tagMetricType] = metricTypeGauge
	intcbtags[id] = tags
}

// RegisterFloatGaugeCallback registers a gauge callback which will be called every
// time metrics are requested.
// There is a maximum of 10240 float callbacks, after which adding a new one will panic.
func RegisterFloatGaugeCallback(name string, tags map[string]string, cb FloatGaugeCallback) {
	id := atomic.AddUint32(curFloatCbID, 1) - 1

	if id >= maxNumCallbacks {
		panic("Too many callbacks")
	}

	floatcallbacks[id] = cb
	floatcbnames[id] = name

	tags[tagMetricType] = metricTypeGauge
	floatcbtags[id] = tags
}

func getAllCallbackGauges() ([]intmetric, []floatmetric) {
	numIDs := int(atomic.LoadUint32(curIntCbID))
	retint := make([]intmetric, numIDs)

	for i := 0; i < numIDs; i++ {
		retint[i] = intmetric{
			name: intcbnames[i],
			val:  intcallbacks[i](),
			tags: intcbtags[i],
		}
	}

	numIDs = int(atomic.LoadUint32(curFloatCbID))
	retfloat := make([]floatmetric, numIDs)

	for i := 0; i < numIDs; i++ {
		retfloat[i] = floatmetric{
			name: floatcbnames[i],
			val:  floatcallbacks[i](),
			tags: floatcbtags[i],
		}
	}

	return retint, retfloat
}
