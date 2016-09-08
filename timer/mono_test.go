// Copyright 2016 Netflix, Inc.
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

package timer_test

import (
	"testing"
	"time"

	"github.com/netflix/rend/timer"
)

func TestMonotonicAndWallTimerDifference(t *testing.T) {
	start := time.Now().UnixNano()
	monostart := timer.Now()

	for i := 0; i < 10; i++ {
		diff := time.Now().UnixNano() - start
		monodiff := timer.Since(monostart)

		t.Logf("Wall: %d", diff)
		t.Logf("Mono: %d", monodiff)

		time.Sleep(1 * time.Second)
	}
}

func TestMonoTimeIsNeverTheSame(t *testing.T) {
	prev := timer.Now()
	same := 0

	for i := 0; i < 100000000; i++ {
		now := timer.Now()
		if now == prev {
			same++
		}
		prev = now
	}

	t.Logf("Times were the same %d times", same)
}

func TestWallTimeCouldBeTheSame(t *testing.T) {
	prev := time.Now()
	same := 0

	for i := 0; i < 100000000; i++ {
		now := time.Now()
		if now.Equal(prev) {
			same++
		}
		prev = now
	}

	t.Logf("Times were the same %d times", same)
}

func TestRegularIntervalsTimer(t *testing.T) {
	for i := 0; i < 100; i++ {
		start := timer.Now()
		time.Sleep(10 * time.Millisecond)
		dur := timer.Since(start)
		t.Logf("10 ms sleep took %d nanoseconds with timer", dur)
	}
}

func TestRegularIntervalsWallTime(t *testing.T) {
	for i := 0; i < 100; i++ {
		start := time.Now().UnixNano()
		time.Sleep(10 * time.Millisecond)
		dur := time.Now().UnixNano() - start
		t.Logf("10 ms sleep took %d nanoseconds with wall-time", dur)
	}
}
