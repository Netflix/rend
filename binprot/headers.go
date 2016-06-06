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

package binprot

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/pool"
)

const ReqHeaderLen = 24

var (
	MetricBinaryRequestHeadersParsed    = metrics.AddCounter("binary_request_headers_parsed")
	MetricBinaryRequestHeadersBadMagic  = metrics.AddCounter("binary_request_headers_bad_magic")
	MetricBinaryResponseHeadersParsed   = metrics.AddCounter("binary_response_headers_parsed")
	MetricBinaryResponseHeadersBadMagic = metrics.AddCounter("binary_response_headers_bad_magic")
)

type RequestHeader struct {
	Magic           uint8 // Already known, since we're here
	Opcode          uint8
	KeyLength       uint16
	ExtraLength     uint8
	DataType        uint8  // Always 0
	VBucket         uint16 // Not used
	TotalBodyLength uint32
	OpaqueToken     uint32 // Echoed to the client
	CASToken        uint64 // Unused in current implementation
}

const resHeaderLen = 24

type ResponseHeader struct {
	Magic           uint8 // always 0x81
	Opcode          uint8
	KeyLength       uint16
	ExtraLength     uint8
	DataType        uint8 // unused, always 0
	Status          uint16
	TotalBodyLength uint32
	OpaqueToken     uint32 // same as the user passed in
	CASToken        uint64
}

func makeRequestHeader(opcode uint8, keyLength, extraLength, totalBodyLength int) RequestHeader {
	rh := reqHeadPool.Get().(RequestHeader)
	rh.Magic = MagicRequest
	rh.Opcode = opcode
	rh.KeyLength = uint16(keyLength)
	rh.ExtraLength = uint8(extraLength)
	rh.DataType = uint8(0)
	rh.VBucket = uint16(0)
	rh.TotalBodyLength = uint32(totalBodyLength)
	rh.OpaqueToken = uint32(0)
	rh.CASToken = uint64(0)

	return rh
}

const (
	headerBufSize   = 24
	headerPoolScale = 10
)

var bufPool = pool.NewFixedSizeBufferPool(headerBufSize, headerPoolScale)

var resHeadPool = &sync.Pool{
	New: func() interface{} {
		return ResponseHeader{}
	},
}

func PutResponseHeader(rh ResponseHeader) {
	resHeadPool.Put(rh)
}

var reqHeadPool = &sync.Pool{
	New: func() interface{} {
		return RequestHeader{}
	},
}

var (
	emptyResHeader = ResponseHeader{}
	emptyReqHeader = RequestHeader{}
)

func readRequestHeader(r io.Reader) (RequestHeader, error) {
	buf, token := bufPool.Get()

	br, err := io.ReadAtLeast(r, buf, ReqHeaderLen)
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(br))
	if err != nil {
		bufPool.Put(token)
		return emptyReqHeader, err
	}

	if buf[0] != MagicRequest {
		bufPool.Put(token)
		metrics.IncCounter(MetricBinaryRequestHeadersBadMagic)
		return emptyReqHeader, ErrBadMagic
	}

	rh := reqHeadPool.Get().(RequestHeader)
	rh.Magic = buf[0]
	rh.Opcode = buf[1]
	rh.KeyLength = binary.BigEndian.Uint16(buf[2:4])
	rh.ExtraLength = buf[4]
	// ignore DataType, unused
	//rh.DataType = buf[5]
	rh.DataType = 0
	// ignore VBucket, unused
	//rh.VBucket = binary.BigEndian.Uint16(buf[6:8])
	rh.VBucket = 0
	rh.TotalBodyLength = binary.BigEndian.Uint32(buf[8:12])
	rh.OpaqueToken = binary.BigEndian.Uint32(buf[12:16])
	// ignore CAS, unused in rend
	//rh.CASToken = binary.BigEndian.Uint64(buf[16:24])
	rh.CASToken = 0

	bufPool.Put(token)
	metrics.IncCounter(MetricBinaryRequestHeadersParsed)

	return rh, nil
}

func writeRequestHeader(w io.Writer, rh RequestHeader) error {
	buf, token := bufPool.Get()

	buf[0] = rh.Magic
	buf[1] = rh.Opcode
	binary.BigEndian.PutUint16(buf[2:4], rh.KeyLength)
	buf[4] = rh.ExtraLength
	// DataType and VBucket are unused
	buf[5] = 0
	buf[6] = 0
	buf[7] = 0
	binary.BigEndian.PutUint32(buf[8:12], rh.TotalBodyLength)
	binary.BigEndian.PutUint32(buf[12:16], rh.OpaqueToken)

	// zero CAS region
	for i := 16; i < 24; i++ {
		buf[i] = 0
	}

	n, err := w.Write(buf)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))
	bufPool.Put(token)
	return err
}

func ReadResponseHeader(r io.Reader) (ResponseHeader, error) {
	buf, token := bufPool.Get()

	br, err := io.ReadAtLeast(r, buf, resHeaderLen)
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(br))
	if err != nil {
		bufPool.Put(token)
		return emptyResHeader, err
	}

	if buf[0] != MagicResponse {
		bufPool.Put(token)
		metrics.IncCounter(MetricBinaryResponseHeadersBadMagic)
		return emptyResHeader, ErrBadMagic
	}

	rh := resHeadPool.Get().(ResponseHeader)
	rh.Magic = buf[0]
	rh.Opcode = buf[1]
	rh.KeyLength = binary.BigEndian.Uint16(buf[2:4])
	rh.ExtraLength = buf[4]
	// ignore DataType, unused
	//rh.DataType = buf[5]
	rh.DataType = 0
	rh.Status = binary.BigEndian.Uint16(buf[6:8])
	rh.TotalBodyLength = binary.BigEndian.Uint32(buf[8:12])
	rh.OpaqueToken = binary.BigEndian.Uint32(buf[12:16])
	// ignore CAS, unused in rend
	//rh.CASToken = binary.BigEndian.Uint64(buf[16:24])
	rh.CASToken = 0

	bufPool.Put(token)
	metrics.IncCounter(MetricBinaryResponseHeadersParsed)

	return rh, nil
}

func writeResponseHeader(w io.Writer, rh ResponseHeader) error {
	buf, token := bufPool.Get()

	buf[0] = rh.Magic
	buf[1] = rh.Opcode
	binary.BigEndian.PutUint16(buf[2:4], rh.KeyLength)
	buf[4] = rh.ExtraLength
	// DataType is unused
	buf[5] = 0
	binary.BigEndian.PutUint16(buf[6:8], rh.Status)
	binary.BigEndian.PutUint32(buf[8:12], rh.TotalBodyLength)
	binary.BigEndian.PutUint32(buf[12:16], rh.OpaqueToken)

	// zero CAS region
	for i := 16; i < 24; i++ {
		buf[i] = 0
	}

	n, err := w.Write(buf)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))
	bufPool.Put(token)
	return err
}
