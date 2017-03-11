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
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/protocol/binprot"
)

var (
	MetricBatchNumBatches      = metrics.AddCounter("batch_num_batches", nil)
	MetricBatchFullBatches     = metrics.AddCounter("batch_full_batches", nil)
	MetricBatchTimedoutBatches = metrics.AddCounter("batch_timedout_batches", nil)
)

type conn struct {
	rw           *bufio.ReadWriter
	rand         *rand.Rand
	batchDelay   time.Duration
	batchSize    uint32
	maxBatchSize *uint32
	avgBatchData *uint64
	reqchan      chan request
	batchchan    chan batch
	expand       chan struct{}
}

type batch struct {
	responses map[uint32]reshandle
	channels  []chan response
}

func newConn(c net.Conn, batchDelay time.Duration, batchSize, readerSize, writerSize uint32, expand chan struct{}) conn {
	r := bufio.NewReaderSize(c, int(readerSize))
	w := bufio.NewWriterSize(c, int(writerSize))

	nc := conn{
		rw:           bufio.NewReadWriter(r, w),
		rand:         rand.New(rand.NewSource(randSeed())),
		batchDelay:   batchDelay,
		batchSize:    batchSize,
		maxBatchSize: new(uint32),
		avgBatchData: new(uint64),
		reqchan:      make(chan request, batchSize),
		batchchan:    make(chan batch, 1),
		expand:       expand,
	}

	go nc.batcher()
	go nc.reader()

	return nc
}

func (c conn) batcher() {
	var req request
	var reqs []request
	var batchTimeout <-chan time.Time
	var timedout bool

	for {
		if batchTimeout == nil {
			batchTimeout = time.After(c.batchDelay)
		}

		select {
		case req = <-c.reqchan:
			timedout = false
			// queue up the request
			reqs = append(reqs, req)

		case <-batchTimeout:
			timedout = true
		}

		// After the batch delay we want to get the requests that do exist moving along
		// Or, if there's enough to batch together, send them off
		if (timedout && len(reqs) > 0) || len(reqs) >= int(c.batchSize) {

			if timedout {
				metrics.IncCounter(MetricBatchTimedoutBatches)
			}
			if len(reqs) >= int(c.batchSize) {
				metrics.IncCounter(MetricBatchFullBatches)
			}

			// store the batch size if it's greater than the maxBatchSize
			// Commented out for now because it's not used in the monitor
			/*
				for {
					max := atomic.LoadUint32(c.maxBatchSize)
					if uint32(len(reqs)) < max || atomic.CompareAndSwapUint32(c.maxBatchSize, max, uint32(len(reqs))) {
						break
					}
				}
			*/

			// Update the average batch size. The uint64 is two packed uint32's
			// The upper 32 bits is the count of the number of batches since the last reset by the monitor
			// The lower 32 bits are the count of the commands sent in all of the batches
			// By packing these into one 64 bit int, we can do the atomic add and increment both counters together atomically
			// This bit arithmetic assumes a batch will never be greater than 2^32 items
			avgUpdate := 1<<32 | (uint64(len(reqs)) & 0xFFFFFFFF)
			atomic.AddUint64(c.avgBatchData, avgUpdate)

			// Set batch timeout channel nil to reset it. Next batch will get a new timeout.
			batchTimeout = nil

			// do
			c.do(reqs)
			reqs = reqs[:0]
		}

		// block until a request comes in if there's a timeout earlier so this doesn't constantly spin
		if timedout {
			req = <-c.reqchan
			reqs = append(reqs, req)

			// Reset timeout variables to base state
			timedout = false
			batchTimeout = nil
		}
	}
}

func (c conn) do(reqs []request) {
	// batch and perform requests
	metrics.IncCounter(MetricBatchNumBatches)
	buf, responses, channels := c.batchIntoBuffer(reqs)

	// send the batch before the write because the write may block for a long time until
	// some responses can be read from memcached.
	c.batchchan <- batch{
		responses: responses,
		channels:  channels,
	}

	// Write out the whole buffer
	n, _ := c.rw.Write(buf.Bytes())
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))
	if err := c.rw.Flush(); err != nil {
		oops(err, responses)
	}

	batcherPool.Put(buf)
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
	println("OOPS", err.Error())
	for _, c := range res {
		c.reschan <- response{err: err}
	}
}

var batcherPool = &sync.Pool{
	New: func() interface{} {
		// 64k by default, may expand with use
		return bytes.NewBuffer(make([]byte, 0, 1<<16))
	},
}

// The opaque value in the requests is related to the base value returned by its index in the array
// meaning the first request sent out will have the opaque value <base>, the second <base+1>, and so on
// The base is a random uint32 value. This is done to prevent possible confusion between requests,
// where one request would get another's data. This would be really bad if we sent things like user
// information to the wrong place.
func (c conn) batchIntoBuffer(reqs []request) (*bytes.Buffer, map[uint32]reshandle, []chan response) {
	// get random base value
	// serialize all requests into a buffer
	// while serializing, collect channels
	// return everything

	// Get base opaque value
	opaque := uint32(c.rand.Int31())
	buf := batcherPool.Get().(*bytes.Buffer)
	buf.Reset()
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
			buf.Write(cmd.Data)
			responses[opaque] = reshandle{
				key:     cmd.Key,
				opaque:  cmd.Opaque,
				quiet:   cmd.Quiet,
				reschan: req.reschan,
			}

		case common.RequestAdd:
			cmd := req.req.(common.SetRequest)
			binprot.WriteAddCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			buf.Write(cmd.Data)
			responses[opaque] = reshandle{
				key:     cmd.Key,
				opaque:  cmd.Opaque,
				quiet:   cmd.Quiet,
				reschan: req.reschan,
			}

		case common.RequestReplace:
			cmd := req.req.(common.SetRequest)
			binprot.WriteReplaceCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			buf.Write(cmd.Data)
			responses[opaque] = reshandle{
				key:     cmd.Key,
				opaque:  cmd.Opaque,
				quiet:   cmd.Quiet,
				reschan: req.reschan,
			}

		case common.RequestAppend:
			cmd := req.req.(common.SetRequest)
			binprot.WriteAppendCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			buf.Write(cmd.Data)
			responses[opaque] = reshandle{
				key:     cmd.Key,
				opaque:  cmd.Opaque,
				quiet:   cmd.Quiet,
				reschan: req.reschan,
			}

		case common.RequestPrepend:
			cmd := req.req.(common.SetRequest)
			binprot.WritePrependCmd(buf, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data)), opaque)
			buf.Write(cmd.Data)
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

	return buf, responses, channels
}

func (c conn) reader() {
	for {

		// read the next batch to process
		batch := <-c.batchchan

		// read in all of the responses
		for len(batch.responses) > 0 {
			resHeader, err := binprot.ReadResponseHeader(c.rw)
			if err != nil {
				oops(err, batch.responses)
			}

			err = binprot.DecodeError(resHeader)
			if err != nil {
				n, ioerr := c.rw.Discard(int(resHeader.TotalBodyLength))
				metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))

				if ioerr != nil {
					oops(ioerr, batch.responses)
				}

				if err != common.ErrKeyNotFound && err != common.ErrKeyExists && err != common.ErrItemNotStored {
					println("UH OH", err.Error())
				}

				// TODO: these are not necessarily all misses, check the error
				if rh, ok := batch.responses[resHeader.OpaqueToken]; ok {
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

					delete(batch.responses, resHeader.OpaqueToken)
					binprot.PutResponseHeader(resHeader)
					continue

				} else {
					panic("FATAL ERROR: Batch out of sync")
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
					oops(err, batch.responses)
				}
				serverFlags := binary.BigEndian.Uint32(b)

				var serverExp uint32
				if resHeader.Opcode == binprot.OpcodeGetE || resHeader.Opcode == binprot.OpcodeGetEQ {
					n, err = io.ReadAtLeast(c.rw, b, 4)
					metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
					if err != nil {
						oops(err, batch.responses)
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
					oops(err, batch.responses)
				}

				if rh, ok := batch.responses[resHeader.OpaqueToken]; ok {
					// send the response back on the channel assigned to this token
					rh.reschan <- response{
						err: nil,
						gr: common.GetEResponse{
							Key:     rh.key,
							Data:    buf,
							Flags:   serverFlags,
							Exptime: serverExp,
							Opaque:  rh.opaque,
							Quiet:   rh.quiet,
						},
					}

					delete(batch.responses, resHeader.OpaqueToken)

				} else {
					// we are out of sync here, something is really wrong. We got the wrong opaque
					// value for the set of requests we thing we sent. We can't fix this, so we have
					// to close the connection
					panic("FATAL ERROR: Batch out of sync")
				}

			} else {
				// Non-get repsonses
				// Discard the message for non-get responses
				n, err := c.rw.Discard(int(resHeader.TotalBodyLength))
				metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
				if err != nil {
					oops(err, batch.responses)
				}

				if rh, ok := batch.responses[resHeader.OpaqueToken]; ok {
					// send the response back on the channel assigned to this token
					rh.reschan <- response{}
					delete(batch.responses, resHeader.OpaqueToken)

				} else {
					panic("FATAL ERROR: Batch out of sync")
				}
			}

			binprot.PutResponseHeader(resHeader)
		}

		// close all of the channels to clean up and guarantee the get loops break
		for _, c := range batch.channels {
			close(c)
		}
	}
}
