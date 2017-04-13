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

package batched

import (
	crand "crypto/rand"
	"encoding/binary"
	"errors"

	"github.com/netflix/rend/common"
)

var (
	errRetryRequestBecauseOfConnectionFailure = errors.New("")
)

type request struct {
	reqtype common.RequestType
	req     common.Request
	reschan chan response
}

type response struct {
	err error
	// a GetEResponse is a superset of all other responses
	gr common.GetEResponse
}

func randSeed() int64 {
	b := make([]byte, 8)
	if _, err := crand.Read(b); err != nil {
		panic(err)
	}
	return int64(binary.LittleEndian.Uint64(b))
}
