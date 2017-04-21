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
	"strconv"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
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

const (
	maxRetryMetrics            = 20
	requestRetryMetricName     = "batch_request_retry"
	requestRetryAttemptTagName = "attempt"
)

var (
	requestRetryMetrics    []uint32
	metricRequestRetryHigh = metrics.AddCounter(
		requestRetryMetricName,
		metrics.Tags{requestRetryAttemptTagName: "high"},
	)
)

func init() {
	for i := range requestRetryMetrics {
		requestRetryMetrics[i] = metrics.AddCounter(
			requestRetryMetricName,
			metrics.Tags{requestRetryAttemptTagName: strconv.Itoa(i)},
		)
	}
}

func (h Handler) doRequest(cmd common.Request, reqType common.RequestType) (common.GetEResponse, error) {
	var res response

	// If we don't try more times than the number of connections, one request may
	// go down the line and cause all the different connections to realize that they
	// are no longer connected. This means the one request sees many more reconnects
	// and might fail very quickly. We can give it a chance to actually succeed by
	// guaranteeing that it sees a connection that has reconnected from this single
	// reconnect phase. This does not guarantee exactly that the request will get sent
	// to the server, only that it does not get rejected from only a single reconnect
	// event.
	conns := h.relay.conns.Load().([]*conn)
	maxTries := len(conns) * 2

	for i := 0; i < maxTries; i++ {
		if i < maxRetryMetrics {
			metrics.IncCounter(requestRetryMetrics[i])
		} else {
			metrics.IncCounter(metricRequestRetryHigh)
		}

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
		if res.err != errRetryRequestBecauseOfConnectionFailure {
			break
		}
	}

	if res.err == errRetryRequestBecauseOfConnectionFailure {
		return common.GetEResponse{}, common.ErrInternal
	}

	return res.gr, res.err
}

// Set performs a set operation on the backend. It unconditionally sets a key to a value.
func (h Handler) Set(cmd common.SetRequest) error {
	_, err := h.doRequest(cmd, common.RequestSet)
	return err
}

// Add performs an add operation on the backend. It only sets the value if it does not already exist.
func (h Handler) Add(cmd common.SetRequest) error {
	_, err := h.doRequest(cmd, common.RequestAdd)
	return err
}

// Replace performs a replace operation on the backend. It only sets the value if it already exists.
func (h Handler) Replace(cmd common.SetRequest) error {
	_, err := h.doRequest(cmd, common.RequestReplace)
	return err
}

// Append performs an append operation on the backend. It will append the data to the value only if it already exists.
func (h Handler) Append(cmd common.SetRequest) error {
	_, err := h.doRequest(cmd, common.RequestAppend)
	return err
}

// Prepend performs a prepend operation on the backend. It will prepend the data to the value only if it already exists.
func (h Handler) Prepend(cmd common.SetRequest) error {
	_, err := h.doRequest(cmd, common.RequestPrepend)
	return err
}

// Delete performs a delete operation on the backend. It will unconditionally remove the value.
func (h Handler) Delete(cmd common.DeleteRequest) error {
	_, err := h.doRequest(cmd, common.RequestDelete)
	return err
}

// Touch performs a touch operation on the backend. It will overwrite the expiration time with a new one.
func (h Handler) Touch(cmd common.TouchRequest) error {
	_, err := h.doRequest(cmd, common.RequestTouch)
	return err
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

// GAT performs a get-and-touch on the backend for the given key. It will retrieve the value while updating the TTL to
// the one supplied.
func (h Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	gr, err := h.doRequest(cmd, common.RequestGat)
	return getEResponseToGetResponse(gr), err
}

type keyAttrs struct {
	key    string
	opaque uint32
	quiet  bool
}

type trackermap map[keyAttrs]int

// There may be more than one request with the same key and a different opaque
// We may also get "malicious" input where multiple requests have the same
// key and opaque. If the quiet value is the same
func getRequestToTrackerMap(cmd common.GetRequest) trackermap {
	tm := make(trackermap)

	for i := range cmd.Keys {
		key := keyAttrs{
			key:    string(cmd.Keys[i]),
			opaque: cmd.Opaques[i],
			quiet:  cmd.Quiet[i],
		}

		// we get the 0 value when the map doesn't contain the data so this
		// i correct even for values that don't yet exist
		count := tm[key]
		tm[key] = count + 1
	}

	return tm
}

func trackerMapToGetRequest(tm trackermap) common.GetRequest {
	ret := common.GetRequest{}
	var end keyAttrs

	for key, count := range tm {
		// the one that is *not* quiet must go at the end
		if !key.quiet {
			end = key
			continue
		}

		for i := 0; i < count; i++ {
			ret.Keys = append(ret.Keys, []byte(key.key))
			ret.Opaques = append(ret.Opaques, key.opaque)
			ret.Quiet = append(ret.Quiet, key.quiet)
		}
	}

	var zeroKeyAttrs keyAttrs
	if end != zeroKeyAttrs {
		ret.Keys = append(ret.Keys, []byte(end.key))
		ret.Opaques = append(ret.Opaques, end.opaque)
		ret.Quiet = append(ret.Quiet, end.quiet)
	}

	return ret
}

const maxGetBatchRetries = 10

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

	tm := getRequestToTrackerMap(cmd)
	conns := h.relay.conns.Load().([]*conn)
	maxTries := len(conns) * 2

	for i := 0; i < maxTries; i++ {
		if i < maxRetryMetrics {
			metrics.IncCounter(requestRetryMetrics[i])
		} else {
			metrics.IncCounter(metricRequestRetryHigh)
		}

		reschan := make(chan response)

		if i > 0 {
			// on a retry we need to generate the subset of keys that were not server the first time around
			// to be resubmitted
			cmd = trackerMapToGetRequest(tm)
		}

		h.relay.submit(h.rand, request{
			req:     cmd,
			reqtype: common.RequestGet,
			reschan: reschan,
		})

		errored := false

		for res := range reschan {
			// after an error, drop all the rest of the responses. The contract of the handler interface
			// says that an error will be the last thing to come through; this is just for safety so the
			// connection is guaranteed to not get blocked.
			if errored {
				continue
			}

			if res.err != nil {
				// On the last go-round we can return the error back to the caller
				// because we will no longer be trying to succeed
				if i == maxTries-1 {
					if res.err == errRetryRequestBecauseOfConnectionFailure {
						errorOut <- common.ErrInternal
					} else {
						errorOut <- res.err
					}
				}
				errored = true
				continue
			}

			key := keyAttrs{
				key:    string(res.gr.Key),
				opaque: res.gr.Opaque,
				quiet:  res.gr.Quiet,
			}

			if count, ok := tm[key]; ok {
				if count == 1 {
					delete(tm, key)
				} else {
					tm[key] = count - 1
				}
			}

			dataOut <- getEResponseToGetResponse(res.gr)
		}

		if len(tm) == 0 {
			break
		}
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

	tm := getRequestToTrackerMap(cmd)
	conns := h.relay.conns.Load().([]*conn)
	maxTries := len(conns) * 2

	for i := 0; i < maxTries; i++ {
		if i < maxRetryMetrics {
			metrics.IncCounter(requestRetryMetrics[i])
		} else {
			metrics.IncCounter(metricRequestRetryHigh)
		}

		reschan := make(chan response)

		if i > 0 {
			// on a retry we need to generate the subset of keys that were not server the first time around
			// to be resubmitted
			cmd = trackerMapToGetRequest(tm)
		}

		h.relay.submit(h.rand, request{
			req:     cmd,
			reqtype: common.RequestGet,
			reschan: reschan,
		})

		errored := false

		for res := range reschan {
			// after an error, drop all the rest of the responses. The contract of the handler interface
			// says that an error will be the last thing to come through; this is just for safety so the
			// connection is guaranteed to not get blocked.
			if errored {
				continue
			}

			if res.err != nil {
				// On the last go-round we can return the error back to the caller
				// because we will no longer be trying to succeed
				if i == maxTries-1 {
					if res.err == errRetryRequestBecauseOfConnectionFailure {
						errorOut <- common.ErrInternal
					} else {
						errorOut <- res.err
					}
				}
				errored = true
				continue
			}

			key := keyAttrs{
				key:    string(res.gr.Key),
				opaque: res.gr.Opaque,
				quiet:  res.gr.Quiet,
			}

			if count, ok := tm[key]; ok {
				if count == 1 {
					delete(tm, key)
				} else {
					tm[key] = count - 1
				}
			}

			dataOut <- res.gr
		}

		if len(tm) == 0 {
			break
		}
	}
}
