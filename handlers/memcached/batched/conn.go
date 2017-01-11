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
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"time"

	"github.com/netflix/rend/binprot"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
)

type conn struct {
	rw           *bufio.ReadWriter
	rand         *rand.Rand
	batchDelay   time.Duration
	batchSize    uint32
	maxBatchSize *uint32
	reqchan      chan request
}

func newConn(c net.Conn, batchDelay time.Duration, batchSize uint32, readerSize, writerSize int) conn {
	println("NEWCONN")

	r := bufio.NewReaderSize(c, readerSize)
	w := bufio.NewWriterSize(c, writerSize)

	nc := conn{
		rw:           bufio.NewReadWriter(r, w),
		rand:         rand.New(rand.NewSource(randSeed())),
		batchDelay:   batchDelay,
		batchSize:    batchSize,
		maxBatchSize: new(uint32),
		reqchan:      make(chan request),
	}

	go nc.loop()

	return nc
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

		case <-time.After(c.batchDelay * time.Microsecond):
			println("TIMEDOUT")
			timedout = true
		}
		println(timedout)
		fmt.Printf("%#v\n", reqs)

		// After 1 millisecond we want to get the requests that do exist moving along
		// Or, if there's enough to batch together, send them off
		if (timedout && len(reqs) > 0) || len(reqs) > int(c.batchSize) {
			c.do(reqs)
		}

		// block until a request comes in if there's a timeout earlier so this doesn't constantly spin
		if timedout {
			println("BLOCKING")
			req = <-c.reqchan
			reqs = append(reqs, req)
			timedout = false
		}
	}
}

func (c conn) do(reqs []request) error {

	// batch and perform requests
	buf, responses, channels := c.batchIntoBuffer(reqs)

	fmt.Printf("%#v\n%#v\n", buf, responses)

	// Write out the whole buffer
	c.rw.Write(buf)
	if err := c.rw.Flush(); err != nil {
		oops(err, responses)
		return err
	}

	println("flushed")

	// read in all of the responses
	for len(responses) > 0 {
		println(len(responses))
		println("READING HEADER")
		resHeader, err := binprot.ReadResponseHeader(c.rw)
		if err != nil {
			oops(err, responses)
			return err
		}
		defer binprot.PutResponseHeader(resHeader)

		println("GOT HEADER")

		err = binprot.DecodeError(resHeader)
		if err != nil {
			println("ERROR NOT NIL")
			n, ioerr := c.rw.Discard(int(resHeader.TotalBodyLength))
			metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))

			if ioerr != nil {
				oops(ioerr, responses)
				return err
			}

			// TODO: these are not necessarily all misses, check the error
			if rh, ok := responses[resHeader.OpaqueToken]; ok {
				println("SENDING MISS")
				// this is an application-level error and should be treated as such
				rh.reschan <- response{
					err: nil,
					gr: common.GetEResponse{
						Miss:   true,
						Quiet:  rh.quiet,
						Opaque: rh.opaque,
						Key:    rh.key,
					},
				}
				println("MISS SENT")

				delete(responses, resHeader.OpaqueToken)
				continue

			} else {
				// FATAL ERROR!!!! no idea what to do here though...
				panic("FATAL ERROR")
			}
		}

		// if reading information (and not just a response header) from the remote
		// process, do some extra parsing
		if resHeader.Opcode == binprot.OpcodeGet ||
			resHeader.Opcode == binprot.OpcodeGetQ ||
			resHeader.Opcode == binprot.OpcodeGat ||
			resHeader.Opcode == binprot.OpcodeGetE ||
			resHeader.Opcode == binprot.OpcodeGetEQ {

			b := make([]byte, 4)
			n, err := io.ReadAtLeast(c.rw, b, 4)
			metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
			if err != nil {
				oops(err, responses)
				return err
			}
			serverFlags := binary.BigEndian.Uint32(b)

			var serverExp uint32
			if resHeader.Opcode == binprot.OpcodeGetE || resHeader.Opcode == binprot.OpcodeGetEQ {
				n, err = io.ReadAtLeast(c.rw, b, 4)
				metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
				if err != nil {
					oops(err, responses)
					return err
				}
				serverExp = binary.BigEndian.Uint32(b)
			}

			// total body - key - extra
			dataLen := resHeader.TotalBodyLength - uint32(resHeader.KeyLength) - uint32(resHeader.ExtraLength)
			buf := make([]byte, dataLen)

			// Read in value
			n, err = io.ReadAtLeast(c.rw, buf, int(dataLen))
			metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
			if err != nil {
				oops(err, responses)
				return err
			}

			if rh, ok := responses[resHeader.OpaqueToken]; ok {
				// send the response back on the channel assigned to this token
				rh.reschan <- response{
					err: nil,
					gr: common.GetEResponse{
						Key:     rh.key,
						Data:    buf,
						Flags:   serverFlags,
						Exptime: serverExp,
					},
				}

			} else {
				// we are out of sync here, something is really wrong. We got the wrong opaque
				// value for the set of requests we thing we sent. We can't fix this, so we have
				// to close the connection
			}

		} else {
			// Non-get repsonses
			// Discard the message for non-get responses
			n, err := c.rw.Discard(int(resHeader.TotalBodyLength))
			metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
			if err != nil {
				oops(err, responses)
				return err
			}
		}
	}

	println("CLOSING CHANNELS")
	for _, c := range channels {
		close(c)
	}

	return nil
}

// Need a struct here to hold all the metadata about the request.
// Responses don't have all the metadata, so it needs to be kept to the side
type reshandle struct {
	key     []byte
	opaque  uint32
	quiet   bool
	reschan chan response
}

// If something goes seriously wrong (i.e. an I/O error on this connection) then send back the
// error to all of the connections waiting on responses from this connection.
func oops(err error, res map[uint32]reshandle) {
	for _, c := range res {
		c.reschan <- response{err: err}
	}
}

// The opaque value in the requests is related to the base value returned by its index in the array
// meaning the first request sent out will have the opaque value <base>, the second <base+1>, and so on
// The base is a random uint32 value. This is done to prevent possible confusion between requests,
// where one request would get another's data. This would be really bad if we sent things like user
// information to the wrong place.
func (c conn) batchIntoBuffer(reqs []request) ([]byte, map[uint32]reshandle, []chan response) {
	// get random base value
	// serialize all requests into a buffer
	// while serializing, collect channels
	// return everything

	// Get base opaque value
	opaque := uint32(c.rand.Int31())
	buf := new(bytes.Buffer)
	responses := make(map[uint32]reshandle)
	channels := make([]chan response, 0, len(reqs))

	for _, req := range reqs {

		// maintain the "sequence number"
		opaque++

		// build slice of channels for closing later
		channels = append(channels, req.reschan)

		// serialize the requests into the buffer. There's no error handling because writes to a bytes.Buffer
		// never return an error.
		switch req.reqtype {
		case common.RequestSet:
			cmd := req.req.(common.SetRequest)
			binprot.WriteSetCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			responses[opaque] = reshandle{
				key:     cmd.Key,
				opaque:  cmd.Opaque,
				quiet:   cmd.Quiet,
				reschan: req.reschan,
			}

		case common.RequestAdd:
			cmd := req.req.(common.SetRequest)
			binprot.WriteAddCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			responses[opaque] = reshandle{
				key:     cmd.Key,
				opaque:  cmd.Opaque,
				quiet:   cmd.Quiet,
				reschan: req.reschan,
			}

		case common.RequestReplace:
			cmd := req.req.(common.SetRequest)
			binprot.WriteReplaceCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			responses[opaque] = reshandle{
				key:     cmd.Key,
				opaque:  cmd.Opaque,
				quiet:   cmd.Quiet,
				reschan: req.reschan,
			}

		case common.RequestAppend:
			cmd := req.req.(common.SetRequest)
			binprot.WriteAppendCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			responses[opaque] = reshandle{
				key:     cmd.Key,
				opaque:  cmd.Opaque,
				quiet:   cmd.Quiet,
				reschan: req.reschan,
			}

		case common.RequestPrepend:
			cmd := req.req.(common.SetRequest)
			binprot.WritePrependCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			responses[opaque] = reshandle{
				key:     cmd.Key,
				opaque:  cmd.Opaque,
				quiet:   cmd.Quiet,
				reschan: req.reschan,
			}

		case common.RequestDelete:
			cmd := req.req.(common.DeleteRequest)
			binprot.WriteDeleteCmd(buf, cmd.Key, opaque)
			responses[opaque] = reshandle{
				key:     cmd.Key,
				opaque:  cmd.Opaque,
				quiet:   cmd.Quiet,
				reschan: req.reschan,
			}

		case common.RequestTouch:
			cmd := req.req.(common.TouchRequest)
			binprot.WriteTouchCmd(buf, cmd.Key, cmd.Exptime, opaque)
			responses[opaque] = reshandle{
				key:     cmd.Key,
				opaque:  cmd.Opaque,
				quiet:   cmd.Quiet,
				reschan: req.reschan,
			}

		case common.RequestGat:
			cmd := req.req.(common.GATRequest)
			binprot.WriteGATCmd(buf, cmd.Key, cmd.Exptime, opaque)
			responses[opaque] = reshandle{
				key:     cmd.Key,
				opaque:  cmd.Opaque,
				quiet:   cmd.Quiet,
				reschan: req.reschan,
			}

		case common.RequestGet:
			cmd := req.req.(common.GetRequest)

			for idx := range cmd.Keys {
				binprot.WriteGetCmd(buf, cmd.Keys[idx], opaque)
				responses[opaque] = reshandle{
					key:     cmd.Keys[idx],
					opaque:  cmd.Opaques[idx],
					quiet:   cmd.Quiet[idx],
					reschan: req.reschan,
				}
				opaque++
			}

		case common.RequestGetE:
			cmd := req.req.(common.GetRequest)

			for idx := range cmd.Keys {
				binprot.WriteGetECmd(buf, cmd.Keys[idx], opaque)
				responses[opaque] = reshandle{
					key:     cmd.Keys[idx],
					opaque:  cmd.Opaques[idx],
					quiet:   cmd.Quiet[idx],
					reschan: req.reschan,
				}
				opaque++
			}
		}
	}

	fmt.Printf("%#v\n", responses)

	return buf.Bytes(), responses, channels
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





func simpleCmdLocal(rw *bufio.ReadWriter) error {
	if err := rw.Flush(); err != nil {
		return err
	}

	resHeader, err := binprot.ReadResponseHeader(rw)
	if err != nil {
		return err
	}
	defer binprot.PutResponseHeader(resHeader)

	err = binprot.DecodeError(resHeader)
	if err != nil {
		n, ioerr := rw.Discard(int(resHeader.TotalBodyLength))
		metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
		if ioerr != nil {
			return ioerr
		}
		return err
	}

	// Read in the message bytes from the body
	n, err := rw.Discard(int(resHeader.TotalBodyLength))
	metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))

	return err
}

func getLocal(rw *bufio.ReadWriter, readExp bool) (data []byte, flags, exp uint32, err error) {
	if err := rw.Flush(); err != nil {
		return nil, 0, 0, err
	}

	resHeader, err := binprot.ReadResponseHeader(rw)
	if err != nil {
		return nil, 0, 0, err
	}
	defer binprot.PutResponseHeader(resHeader)

	err = binprot.DecodeError(resHeader)
	if err != nil {
		n, ioerr := rw.Discard(int(resHeader.TotalBodyLength))
		metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
		if ioerr != nil {
			return nil, 0, 0, ioerr
		}
		return nil, 0, 0, err
	}

	var serverFlags uint32
	binary.Read(rw, binary.BigEndian, &serverFlags)
	metrics.IncCounterBy(common.MetricBytesReadLocal, 4)

	var serverExp uint32
	if readExp {
		binary.Read(rw, binary.BigEndian, &serverExp)
		metrics.IncCounterBy(common.MetricBytesReadLocal, 4)
	}

	// total body - key - extra
	dataLen := resHeader.TotalBodyLength - uint32(resHeader.KeyLength) - uint32(resHeader.ExtraLength)
	buf := make([]byte, dataLen)

	// Read in value
	n, err := io.ReadAtLeast(rw, buf, int(dataLen))
	metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
	if err != nil {
		return nil, 0, 0, err
	}

	return buf, serverFlags, serverExp, nil
}







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
