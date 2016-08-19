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
	ctags        = make([]map[string]string, maxNumCounters)
	curCounterID = new(uint32)
)

// Registers a counter and returns an ID that can be used to access it
// There is a maximum of 1024 metrics, after which adding a new one will panic
func AddCounter(name string, tags map[string]string) uint32 {
	id := atomic.AddUint32(curCounterID, 1) - 1

	if id >= maxNumCounters {
		panic("Too many counters")
	}

	cnames[id] = name

	tags[tagType] = typeCounter
	ctags[id] = tags

	return id
}

func IncCounter(id uint32) {
	atomic.AddUint64(&counters[id], 1)
}

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
			tags: ctags[i],
		}
	}

	return ret
}
