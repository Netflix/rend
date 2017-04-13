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

package batched

import (
	"math/rand"

	"github.com/netflix/rend/common"
)

// Handler implements the handlers.Handler interface. It is an implementation of the interface
// that defers all requests to a connection pool to the backend. This is done in order to decrease
// the number of syscalls that the process has to make by batching requests as much as possible
// per connection. The connection pool is a grow-only style where more connections may be added as
// needed but they are never torn down.
type Handler struct {
	relay *relay
	rand  *rand.Rand
}

// Opts is the set of tuning options for the batched handler.
type Opts struct {
	BatchSize             uint32
	BatchDelayMicros      uint32
	ReadBufSize           uint32
	WriteBufSize          uint32
	EvaluationIntervalSec uint32
	LoadFactorExpandRatio float64
	OverloadedConnRatio   float64
}

var defaultOpts = Opts{
	BatchSize:             10,
	BatchDelayMicros:      250,
	ReadBufSize:           1 << 16, // 64k
	WriteBufSize:          1 << 16, // 64k
	EvaluationIntervalSec: 2,
	LoadFactorExpandRatio: 0.75,
	OverloadedConnRatio:   0.2,
}

func uint32ValueOrDefault(val uint32, def uint32) uint32 {
	if val <= 0 {
		return def
	}
	return val
}

func float64ValueOrDefault(val float64, def float64) float64 {
	if val <= 0 {
		return def
	}
	return val
}

// NewHandler creates a new handler with the given unix socket as the connected backend. The first
// time this method is called it creates a background monitor that will add connections as needed
// for the given domain socket. The Opts parameter can exclude any settings in order to take the
// defaults. Any setting that is at the 0 value or negative will take the default.
//
// Default values are:
//
// BatchSize:             10,
// BatchDelayMicros:      250,
// ReadBufSize:           1 << 16, // 64k
// WriteBufSize:          1 << 16, // 64k
// EvaluationIntervalSec: 2,
// LoadFactorExpandRatio: 0.75,
// OverloadedConnRatio:   0.2,
func NewHandler(sock string, opts Opts) Handler {
	io := Opts{
		BatchSize:             uint32ValueOrDefault(opts.BatchSize, defaultOpts.BatchSize),
		BatchDelayMicros:      uint32ValueOrDefault(opts.BatchDelayMicros, defaultOpts.BatchDelayMicros),
		ReadBufSize:           uint32ValueOrDefault(opts.ReadBufSize, defaultOpts.ReadBufSize),
		WriteBufSize:          uint32ValueOrDefault(opts.WriteBufSize, defaultOpts.WriteBufSize),
		EvaluationIntervalSec: uint32ValueOrDefault(opts.EvaluationIntervalSec, defaultOpts.EvaluationIntervalSec),
		LoadFactorExpandRatio: float64ValueOrDefault(opts.LoadFactorExpandRatio, defaultOpts.LoadFactorExpandRatio),
		OverloadedConnRatio:   float64ValueOrDefault(opts.OverloadedConnRatio, defaultOpts.OverloadedConnRatio),
	}

	return Handler{
		relay: getRelay(sock, io),
		rand:  rand.New(rand.NewSource(randSeed())),
	}
}

// Close does nothing for this Handler as the connections are pooled behind it and are not explicitly controlled.
func (h Handler) Close() error {
	return nil
}

const requestTries = 3

func (h Handler) doSimpleRequest(cmd common.Request, reqType common.RequestType) error {
	for i := 0; i < 3; i++ {
		reschan := make(chan response)

		h.relay.submit(h.rand, request{
			req:     cmd,
			reqtype: reqType,
			reschan: reschan,
		})

		// wait for the response from the pool over the response channel
		// and return whatever it gives as the error
		res := <-reschan

		// If the connection signals that the connection failed, we should retry
		// a few times as connections get recreated
		if res.err == errRetryRequestBecauseOfConnectionFailure {
			// TODO: increment metric
			continue
		}
		return res.err
	}
}

// Set performs a set operation on the backend. It unconditionally sets a key to a value.
func (h Handler) Set(cmd common.SetRequest) error {
	return h.doSimpleRequest(cmd, common.RequestSet)
}

// Add performs an add operation on the backend. It only sets the value if it does not already exist.
func (h Handler) Add(cmd common.SetRequest) error {
	reschan := make(chan response)

	h.relay.submit(h.rand, request{
		req:     cmd,
		reqtype: common.RequestAdd,
		reschan: reschan,
	})

	res := <-reschan
	return res.err
}

// Replace performs a replace operation on the backend. It only sets the value if it already exists.
func (h Handler) Replace(cmd common.SetRequest) error {
	reschan := make(chan response)

	h.relay.submit(h.rand, request{
		req:     cmd,
		reqtype: common.RequestReplace,
		reschan: reschan,
	})

	res := <-reschan
	return res.err
}

// Append performs an append operation on the backend. It will append the data to the value only if it already exists.
func (h Handler) Append(cmd common.SetRequest) error {
	reschan := make(chan response)

	h.relay.submit(h.rand, request{
		req:     cmd,
		reqtype: common.RequestAppend,
		reschan: reschan,
	})

	res := <-reschan
	return res.err
}

// Prepend performs a prepend operation on the backend. It will prepend the data to the value only if it already exists.
func (h Handler) Prepend(cmd common.SetRequest) error {
	reschan := make(chan response)

	h.relay.submit(h.rand, request{
		req:     cmd,
		reqtype: common.RequestPrepend,
		reschan: reschan,
	})

	res := <-reschan
	return res.err
}

func getEResponseToGetResponse(res common.GetEResponse) common.GetResponse {
	return common.GetResponse{
		Key:    res.Key,
		Data:   res.Data,
		Flags:  res.Flags,
		Opaque: res.Opaque,
		Quiet:  res.Quiet,
		Miss:   res.Miss,
	}
}

// Get performs a get operation on the backend. It retrieves the whole of the batch of keys given as a group and returns
// them one at a time over the request channel.
func (h Handler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	dataOut := make(chan common.GetResponse)
	errorOut := make(chan error)
	go realHandleGet(h, cmd, dataOut, errorOut)
	return dataOut, errorOut
}

func realHandleGet(h Handler, cmd common.GetRequest, dataOut chan common.GetResponse, errorOut chan error) {
	defer close(errorOut)
	defer close(dataOut)

	reschan := make(chan response)

	h.relay.submit(h.rand, request{
		req:     cmd,
		reqtype: common.RequestGet,
		reschan: reschan,
	})

	var errored bool

	for res := range reschan {
		if res.err != nil {
			errorOut <- res.err
			errored = true
		}

		// after an error, drop all the rest of the responses. The contract of the handler interface
		// says that an error will be the last thing to come through.
		if errored {
			continue
		}

		dataOut <- getEResponseToGetResponse(res.gr)
	}
}

// GetE performs a get-with-expiration on the backend. It is a custom command only implemented in Rend. It retrieves the
// whole batch of keys given as a group and returns them one at a time over the request channel.
func (h Handler) GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error) {
	dataOut := make(chan common.GetEResponse)
	errorOut := make(chan error)
	go realHandleGetE(h, cmd, dataOut, errorOut)
	return dataOut, errorOut
}

func realHandleGetE(h Handler, cmd common.GetRequest, dataOut chan common.GetEResponse, errorOut chan error) {
	defer close(errorOut)
	defer close(dataOut)

	reschan := make(chan response)

	h.relay.submit(h.rand, request{
		req:     cmd,
		reqtype: common.RequestGet,
		reschan: reschan,
	})

	var errored bool

	for res := range reschan {
		if res.err != nil {
			errorOut <- res.err
			errored = true
		}

		// after an error, drop all the rest of the responses. The contract of the handler interface
		// says that an error will be the last thing to come through.
		if errored {
			continue
		}

		dataOut <- res.gr
	}
}

// GAT performs a get-and-touch on the backend for the given key. It will retrieve the value while updating the TTL to
// the one supplied.
func (h Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	reschan := make(chan response)

	h.relay.submit(h.rand, request{
		req:     cmd,
		reqtype: common.RequestGat,
		reschan: reschan,
	})

	res := <-reschan
	return getEResponseToGetResponse(res.gr), res.err
}

// Delete performs a delete operation on the backend. It will unconditionally remove the value.
func (h Handler) Delete(cmd common.DeleteRequest) error {
	reschan := make(chan response)

	h.relay.submit(h.rand, request{
		req:     cmd,
		reqtype: common.RequestDelete,
		reschan: reschan,
	})

	res := <-reschan
	return res.err
}

// Touch performs a touch operation on the backend. It will overwrite the expiration time with a new one.
func (h Handler) Touch(cmd common.TouchRequest) error {
	reschan := make(chan response)

	h.relay.submit(h.rand, request{
		req:     cmd,
		reqtype: common.RequestTouch,
		reschan: reschan,
	})

	res := <-reschan
	return res.err
}
