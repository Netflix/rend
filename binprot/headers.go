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
	"io"
	"sync"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
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
	rh.Opcode = uint8(opcode)
	rh.KeyLength = uint16(keyLength)
	rh.ExtraLength = uint8(extraLength)
	rh.DataType = uint8(0)
	rh.VBucket = uint16(0)
	rh.TotalBodyLength = uint32(totalBodyLength)
	rh.OpaqueToken = uint32(0)
	rh.CASToken = uint64(0)

	return rh
}

var bufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 24, 24)
	},
}

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
	buf := bufPool.Get().([]byte)

	br, err := io.ReadAtLeast(r, buf, ReqHeaderLen)
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(br))
	if err != nil {
		bufPool.Put(buf)
		return emptyReqHeader, err
	}

	if buf[0] != MagicRequest {
		bufPool.Put(buf)
		metrics.IncCounter(MetricBinaryRequestHeadersBadMagic)
		return emptyReqHeader, ErrBadMagic
	}

	rh := reqHeadPool.Get().(RequestHeader)
	rh.Magic = buf[0]
	rh.Opcode = buf[1]
	rh.KeyLength = uint16(buf[2])<<8 | uint16(buf[3])
	rh.ExtraLength = buf[4]
	// ignore DataType, unused
	//rh.DataType = buf[5]
	rh.DataType = 0
	// ignore VBucket, unused
	//rh.VBucket = uint16(buf[6]) << 8 | uint16(buf[7])
	rh.VBucket = 0
	rh.TotalBodyLength = uint32(buf[8])<<24 | uint32(buf[9])<<16 | uint32(buf[10])<<8 | uint32(buf[11])
	rh.OpaqueToken = uint32(buf[12])<<24 | uint32(buf[13])<<16 | uint32(buf[14])<<8 | uint32(buf[15])
	// ignore CAS, unused in rend
	//rh.CASToken = uint64(buf[16]) << 56 | uint64(buf[17]) << 48 | uint64(buf[18]) << 40 | uint64(buf[19]) << 32 |
	//              uint64(buf[20]) << 24 | uint64(buf[21]) << 16 | uint64(buf[22]) << 8 | uint64(buf[23])
	rh.CASToken = 0

	bufPool.Put(buf)
	metrics.IncCounter(MetricBinaryRequestHeadersParsed)

	return rh, nil
}

func writeRequestHeader(w io.Writer, rh RequestHeader) error {
	buf := bufPool.Get().([]byte)

	buf[0] = rh.Magic
	buf[1] = rh.Opcode
	buf[2] = uint8(rh.KeyLength >> 8)
	buf[3] = uint8(rh.KeyLength)
	buf[4] = rh.ExtraLength
	// DataType and VBucket are unused
	buf[5] = 0
	buf[6] = 0
	buf[7] = 0
	buf[8] = uint8(rh.TotalBodyLength >> 24)
	buf[9] = uint8(rh.TotalBodyLength >> 16)
	buf[10] = uint8(rh.TotalBodyLength >> 8)
	buf[11] = uint8(rh.TotalBodyLength)
	buf[12] = uint8(rh.OpaqueToken >> 24)
	buf[13] = uint8(rh.OpaqueToken >> 16)
	buf[14] = uint8(rh.OpaqueToken >> 8)
	buf[15] = uint8(rh.OpaqueToken)

	// zero CAS region
	for i := 16; i < 24; i++ {
		buf[i] = 0
	}

	n, err := w.Write(buf)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))
	bufPool.Put(buf)
	return err
}

func ReadResponseHeader(r io.Reader) (ResponseHeader, error) {
	buf := bufPool.Get().([]byte)

	br, err := io.ReadAtLeast(r, buf, resHeaderLen)
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(br))
	if err != nil {
		bufPool.Put(buf)
		return emptyResHeader, err
	}

	if buf[0] != MagicResponse {
		bufPool.Put(buf)
		metrics.IncCounter(MetricBinaryResponseHeadersBadMagic)
		return emptyResHeader, ErrBadMagic
	}

	rh := resHeadPool.Get().(ResponseHeader)
	rh.Magic = buf[0]
	rh.Opcode = buf[1]
	rh.KeyLength = uint16(buf[2])<<8 | uint16(buf[3])
	rh.ExtraLength = buf[4]
	// ignore DataType, unused
	//rh.DataType = buf[5]
	rh.DataType = 0
	rh.Status = uint16(buf[6])<<8 | uint16(buf[7])
	rh.TotalBodyLength = uint32(buf[8])<<24 | uint32(buf[9])<<16 | uint32(buf[10])<<8 | uint32(buf[11])
	rh.OpaqueToken = uint32(buf[12])<<24 | uint32(buf[13])<<16 | uint32(buf[14])<<8 | uint32(buf[15])
	// ignore CAS, unused in rend
	//rh.CASToken = uint64(buf[16]) << 56 | uint64(buf[17]) << 48 | uint64(buf[18]) << 40 | uint64(buf[19]) << 32 |
	//                     uint64(buf[20]) << 24 | uint64(buf[21]) << 16 | uint64(buf[22]) << 8 | uint64(buf[23])
	rh.CASToken = 0

	bufPool.Put(buf)
	metrics.IncCounter(MetricBinaryResponseHeadersParsed)

	return rh, nil
}

func writeResponseHeader(w io.Writer, rh ResponseHeader) error {
	buf := bufPool.Get().([]byte)

	buf[0] = rh.Magic
	buf[1] = rh.Opcode
	buf[2] = uint8(rh.KeyLength >> 8)
	buf[3] = uint8(rh.KeyLength)
	buf[4] = rh.ExtraLength
	// DataType is unused
	buf[5] = 0
	buf[6] = uint8(rh.Status >> 8)
	buf[7] = uint8(rh.Status)
	buf[8] = uint8(rh.TotalBodyLength >> 24)
	buf[9] = uint8(rh.TotalBodyLength >> 16)
	buf[10] = uint8(rh.TotalBodyLength >> 8)
	buf[11] = uint8(rh.TotalBodyLength)
	buf[12] = uint8(rh.OpaqueToken >> 24)
	buf[13] = uint8(rh.OpaqueToken >> 16)
	buf[14] = uint8(rh.OpaqueToken >> 8)
	buf[15] = uint8(rh.OpaqueToken)

	// zero CAS region
	for i := 16; i < 24; i++ {
		buf[i] = 0
	}

	n, err := w.Write(buf)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))
	bufPool.Put(buf)
	return err
}
