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
	"bytes"
	"math/rand"
	"time"

	"github.com/netflix/rend/binprot"
	"github.com/netflix/rend/common"
)

type conn struct {
	rand    *rand.Rand
	reqchan chan request
}

func newConn() conn {
	return conn{
		rand:    rand.New(rand.NewSource(randSeed())),
		reqchan: make(chan request),
	}
}

func (c conn) loop() {
	var req request
	var reqs []request
	var timedout bool

	for {
		select {
		case req = <-c.reqchan:
			timedout = false
			// queue up the request
			reqs = append(reqs, req)

		case <-time.After(500 * time.Microsecond):
			timedout = true
		}

		// After 1 millisecond we want to get the requests that do exist moving along
		// Or, if there's enough to batch together, send them off
		if timedout || len(reqs) > 5 {
			// batch and perform requests
		}

		// need a way to block until a request comes in if there's a timeout earlier
		if timedout {
			req = <-c.reqchan
			timedout = false
		}
	}
}

// The opaque value in the requests is related to the base value returned by its index in the array
// meaning the first request sent out will have the opaque value <base>, the second <base+1>, and so on
// The base is a random uint32 value. This is done to prevent possible confusion between requests,
// where one request would get another's data. This would be really bad if we sent things like user
// information to the wrong place.
func (c conn) batchIntoBuffer(reqs []request) ([]byte, map[uint32]chan response) {
	// get random base value
	// serialize all requests into a buffer
	// while serializing, collect channels
	// return everything

	// Get base opaque value
	opaque := uint32(c.rand.Int31())
	buf := new(bytes.Buffer)
	response := make(map[uint32]chan response)

	for _, req := range reqs {

		// maintain the "sequence number"
		opaque++

		// serialize the requests into the buffer. There's no error handling because writes to a bytes.Buffer
		// never return an error.
		switch req.reqtype {
		case common.RequestSet:
			cmd := req.req.(common.SetRequest)
			binprot.WriteSetCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			response[opaque] = req.reschan

		case common.RequestAdd:
			cmd := req.req.(common.SetRequest)
			binprot.WriteAddCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			response[opaque] = req.reschan

		case common.RequestReplace:
			cmd := req.req.(common.SetRequest)
			binprot.WriteReplaceCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			response[opaque] = req.reschan

		case common.RequestAppend:
			cmd := req.req.(common.SetRequest)
			binprot.WriteAppendCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			response[opaque] = req.reschan

		case common.RequestPrepend:
			cmd := req.req.(common.SetRequest)
			binprot.WritePrependCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			response[opaque] = req.reschan

		case common.RequestDelete:
			cmd := req.req.(common.DeleteRequest)
			binprot.WriteDeleteCmd(buf, cmd.Key, opaque)
			response[opaque] = req.reschan

		case common.RequestTouch:
			cmd := req.req.(common.TouchRequest)
			binprot.WriteTouchCmd(buf, cmd.Key, cmd.Exptime, opaque)
			response[opaque] = req.reschan

		case common.RequestGat:
			cmd := req.req.(common.GATRequest)
			binprot.WriteGATCmd(buf, cmd.Key, cmd.Exptime, opaque)
			response[opaque] = req.reschan

		case common.RequestGet:
			cmd := req.req.(common.GetRequest)

			for _, key := range cmd.Keys {
				binprot.WriteGetCmd(buf, key, opaque)
				response[opaque] = req.reschan
				opaque++
			}

		case common.RequestGetE:
			cmd := req.req.(common.GetRequest)

			for _, key := range cmd.Keys {
				binprot.WriteGetECmd(buf, key, opaque)
				response[opaque] = req.reschan
				opaque++
			}
		}
	}

	return buf.Bytes(), response
}

/*
	return h.handleSetCommon(cmd)




GAT:

			data, flags, _, err := getLocal(h.rw, false)
			if err != nil {
				if err == common.ErrKeyNotFound {
					return common.GetResponse{
						Miss:   true,
						Quiet:  false,
						Opaque: cmd.Opaque,
						Flags:  flags,
						Key:    cmd.Key,
						Data:   nil,
					}, nil
				}

				return common.GetResponse{}, err
			}

			return common.GetResponse{
				Miss:   false,
				Quiet:  false,
				Opaque: cmd.Opaque,
				Flags:  flags,
				Key:    cmd.Key,
				Data:   data,
			}, nil





func (h Handler) handleSetCommon(cmd common.SetRequest) error {
	// TODO: should there be a unique flags value for regular data?

	// Write value
	h.rw.Write(cmd.Data)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(len(cmd.Data)))

	if err := h.rw.Flush(); err != nil {
		return err
	}

	// Read server's response
	resHeader, err := readResponseHeader(h.rw.Reader)
	if err != nil {
		// Discard response body
		n, ioerr := h.rw.Discard(int(resHeader.TotalBodyLength))
		metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
		if ioerr != nil {
			return ioerr
		}

		// For Add and Replace, the error here will be common.ErrKeyExists or common.ErrKeyNotFound
		// respectively. For each, this is the right response to send to the requestor. The error
		// here is overloaded because it would signal a true error for sets, but a normal "error"
		// response for Add and Replace.
		return err
	}

	return nil
}

func (h Handler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	dataOut := make(chan common.GetResponse)
	errorOut := make(chan error)
	go realHandleGet(cmd, dataOut, errorOut, h.rw)
	return dataOut, errorOut
}

func realHandleGet(cmd common.GetRequest, dataOut chan common.GetResponse, errorOut chan error, rw *bufio.ReadWriter) {
	defer close(errorOut)
	defer close(dataOut)

	for idx, key := range cmd.Keys {
		if err := binprot.WriteGetCmd(rw.Writer, key); err != nil {
			errorOut <- err
			return
		}

		data, flags, _, err := getLocal(rw, false)
		if err != nil {
			if err == common.ErrKeyNotFound {
				dataOut <- common.GetResponse{
					Miss:   true,
					Quiet:  cmd.Quiet[idx],
					Opaque: cmd.Opaques[idx],
					Flags:  flags,
					Key:    key,
					Data:   nil,
				}

				continue
			}

			errorOut <- err
			return
		}

		dataOut <- common.GetResponse{
			Miss:   false,
			Quiet:  cmd.Quiet[idx],
			Opaque: cmd.Opaques[idx],
			Flags:  flags,
			Key:    key,
			Data:   data,
		}
	}
}

func (h Handler) GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error) {
	dataOut := make(chan common.GetEResponse)
	errorOut := make(chan error)
	go realHandleGetE(cmd, dataOut, errorOut, h.rw)
	return dataOut, errorOut
}

func realHandleGetE(cmd common.GetRequest, dataOut chan common.GetEResponse, errorOut chan error, rw *bufio.ReadWriter) {
	defer close(errorOut)
	defer close(dataOut)

	for idx, key := range cmd.Keys {
		if err := binprot.WriteGetECmd(rw.Writer, key); err != nil {
			errorOut <- err
			return
		}

		data, flags, exp, err := getLocal(rw, true)
		if err != nil {
			if err == common.ErrKeyNotFound {
				dataOut <- common.GetEResponse{
					Miss:    true,
					Quiet:   cmd.Quiet[idx],
					Opaque:  cmd.Opaques[idx],
					Flags:   flags,
					Exptime: exp,
					Key:     key,
					Data:    nil,
				}

				continue
			}

			errorOut <- err
			return
		}

		dataOut <- common.GetEResponse{
			Miss:    false,
			Quiet:   cmd.Quiet[idx],
			Opaque:  cmd.Opaques[idx],
			Flags:   flags,
			Exptime: exp,
			Key:     key,
			Data:    data,
		}
	}
}

*/
