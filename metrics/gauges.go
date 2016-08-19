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

import (
	"math"
	"sync/atomic"
)

const maxNumGauges = 1024

var (
	curIntGaugeID = new(uint32)
	intgnames     = make([]string, maxNumGauges)
	intgauges     = make([]uint64, maxNumGauges)
	intgtags      = make([]map[string]string, maxNumGauges)

	curFloatGaugeID = new(uint32)
	floatgnames     = make([]string, maxNumGauges)
	floatgauges     = make([]uint64, maxNumGauges)
	floatgtags      = make([]map[string]string, maxNumGauges)
)

func init() {
	atomic.StoreUint32(curIntGaugeID, 0)
	atomic.StoreUint32(curFloatGaugeID, 0)
}

// AddIntGauge registers an integer-based gauge and returns an ID that can be
// used to update it.
// There is a maximum of 1024 gauges, after which adding a new one will panic
func AddIntGauge(name string, tags map[string]string) uint32 {
	id := atomic.AddUint32(curIntGaugeID, 1) - 1

	if id >= maxNumGauges {
		panic("Too many gauges")
	}

	intgnames[id] = name

	tags[tagType] = typeGauge
	intgtags[id] = tags

	return id
}

// AddFloatGauge registers a float-based gauge and returns an ID that can be
// used to access it.
// There is a maximum of 1024 gauges, after which adding a new one will panic
func AddFloatGauge(name string, tags map[string]string) uint32 {
	id := atomic.AddUint32(curFloatGaugeID, 1) - 1

	if id >= maxNumGauges {
		panic("Too many gauges")
	}

	floatgnames[id] = name

	tags[tagType] = typeGauge
	floatgtags[id] = tags

	return id
}

// SetIntGauge sets a gauge by the ID returned from AddIntGauge to the value given.
func SetIntGauge(id uint32, value uint64) {
	atomic.StoreUint64(&intgauges[id], value)
}

// SetFloatGauge sets a gauge by the ID returned from AddFloatGauge to the value given.
func SetFloatGauge(id uint32, value float64) {
	// The float64 value needs to be converted into an int64 here because
	// there is no atomic store for float values. This is a literal
	// reinterpretation of the same exact bits.
	v2 := math.Float64bits(value)
	atomic.StoreUint64(&floatgauges[id], v2)
}

func getAllGauges() ([]intmetric, []floatmetric) {
	numIDs := int(atomic.LoadUint32(curIntGaugeID))
	retint := make([]intmetric, numIDs)

	for i := 0; i < numIDs; i++ {
		retint[i] = intmetric{
			name: intgnames[i],
			val:  atomic.LoadUint64(&intgauges[i]),
			tags: intgtags[i],
		}
	}

	numIDs = int(atomic.LoadUint32(curFloatGaugeID))
	retfloat := make([]floatmetric, numIDs)

	for i := 0; i < numIDs; i++ {
		// The int64 bit pattern of the float value needs to be converted back
		// into a float64 here. This is a literal reinterpretation of the same
		// exact bits.
		intval := atomic.LoadUint64(&floatgauges[i])
		floatval := math.Float64frombits(intval)

		retfloat[i] = floatmetric{
			name: floatgnames[i],
			val:  math.Float64frombits(intval),
			tags: floatgtags[i],
		}
	}

	return retint, retfloat
}
