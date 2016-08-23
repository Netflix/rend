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

const maxNumCounters = 1024

var (
	cnames       = make([]string, maxNumCounters)
	counters     = make([]uint64, maxNumCounters)
	ctags        = make([]tags, maxNumCounters)
	curCounterID = new(uint32)
)

// AddCounter registers a counter and returns an ID that can be used to increment it.
// There is a maximum of 1024 metrics, after which adding a new one will panic.
//
// It is recommended to have AddCounter calls as var declarations in any package that
// uses it. E.g.:
//
//   var (
//       MetricFoo = metrics.AddCounter("foo", tags{"tag1": "value"})
//   )
//
// Then in code:
//
//   metrics.IncCounter(MetricFoo)
func AddCounter(name string, tgs tags) uint32 {
	id := atomic.AddUint32(curCounterID, 1) - 1

	if id >= maxNumCounters {
		panic("Too many counters")
	}

	cnames[id] = name

	tgs[tagMetricType] = metricTypeCounter
	tgs[tagDataType] = dataTypeUint64
	ctags[id] = tgs

	return id
}

// IncCounter increases the specified counter by 1. Only values returned by AddCounter
// can be used. Arbitrary values may panic or have undefined behavior.
func IncCounter(id uint32) {
	atomic.AddUint64(&counters[id], 1)
}

// IncCounterBy increments the specified counter by the given amount. This is for situations
// where the count is not a one by one thing, like counting bytes in and out of a system.
func IncCounterBy(id uint32, amount uint64) {
	atomic.AddUint64(&counters[id], amount)
}

func getAllCounters() []intmetric {
	numIDs := int(atomic.LoadUint32(curCounterID))
	ret := make([]intmetric, numIDs)

	for i := 0; i < numIDs; i++ {
		ret[i] = intmetric{
			name: cnames[i],
			val:  atomic.LoadUint64(&counters[i]),
			tgs:  ctags[i],
		}
	}

	return ret
}
