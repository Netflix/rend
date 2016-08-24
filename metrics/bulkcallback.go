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

// BulkCallback defines a function that the metrics package can use to retrieve a set
// of int and float metrics in bulk. Implementors are responsible for setting up tags
// appropriately.
//
// The use case for this type of callback is systems that make retrieving metrics expensive.
// If the metrics call has to go through CGo or is only retrievable in bulk, this type of
// callback will allow an implementor to do so more easily than the individual callbacks.
type BulkCallback func() ([]IntMetric, []FloatMetric)

const maxNumBulkCallbacks = 1024

var (
	curBulkCbID = new(uint32)
	bulkCBs     = make([]BulkCallback, maxNumBulkCallbacks)
)

// RegisterBulkCallback registers a bulk callback which will be called every time
// metrics are requested.
// There is a maximum of 1024 bulk callbacks, after which adding a new one will panic.
func RegisterBulkCallback(bcb BulkCallback) {
	id := atomic.AddUint32(curBulkCbID, 1) - 1

	if id >= maxNumCallbacks {
		panic("Too many callbacks")
	}

	bulkCBs[id] = bcb
}

func getAllBulkCallbackGauges() ([]IntMetric, []FloatMetric) {
	numIDs := int(atomic.LoadUint32(curBulkCbID))
	var intret []IntMetric
	var floatret []FloatMetric

	for i := 0; i < numIDs; i++ {
		ti, tf := bulkCBs[i]()
		intret = append(intret, ti...)
		floatret = append(floatret, tf...)
	}

	return intret, floatret
}
