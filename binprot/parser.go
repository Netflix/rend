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
	"bufio"
	"encoding/binary"
	"io"
	"log"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
)

// Example Set Request
// Field        (offset) (value)
//     Magic        (0)    : 0x80
//     Opcode       (1)    : 0x02
//     Key length   (2,3)  : 0x0005
//     Extra length (4)    : 0x08
//     Data type    (5)    : 0x00
//     VBucket      (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000012
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              :
//       Flags      (24-27): 0xdeadbeef
//       Expiry     (28-31): 0x00000e10
//     Key          (32-36): The textual string "Hello"
//     Value        (37-41): The textual string "World"

// Example Get request
// Field        (offset) (value)
//     Magic        (0)    : 0x80
//     Opcode       (1)    : 0x00
//     Key length   (2,3)  : 0x0005
//     Extra length (4)    : 0x00
//     Data type    (5)    : 0x00
//     VBucket      (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000005 (for "Hello")
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              : None
//     Key          (24-29): The string key (e.g. "Hello")
//     Value               : None

// Example GetQ request
// Field        (offset) (value)
//     Magic        (0)    : 0x80
//     Opcode       (1)    : 0x0d
//     Key length   (2,3)  : 0x0005
//     Extra length (4)    : 0x00
//     Data type    (5)    : 0x00
//     VBucket      (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000005 (for "Hello")
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              : None
//     Key          (24-29): The string key (e.g. "Hello")
//     Value               : None

// Example Delete request
// Field        (offset) (value)
//     Magic        (0)    : 0x80
//     Opcode       (1)    : 0x04
//     Key length   (2,3)  : 0x0005
//     Extra length (4)    : 0x00
//     Data type    (5)    : 0x00
//     VBucket      (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000005
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              : None
//     Key                 : The textual string "Hello"
//     Value               : None

// Example Touch request (not from docs)
// Field        (offset) (value)
//     Magic        (0)    : 0x80
//     Opcode       (1)    : 0x04
//     Key length   (2,3)  : 0x0005
//     Extra length (4)    : 0x04
//     Data type    (5)    : 0x00
//     VBucket      (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000005
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              :
//       Expiry     (24-27): 0x00000e10
//     Key                 : The textual string "Hello"
//     Value               : None

type BinaryParser struct {
	reader *bufio.Reader
}

func NewBinaryParser(reader *bufio.Reader) BinaryParser {
	return BinaryParser{
		reader: reader,
	}
}

// Gets can be pipelined by sending many headers at once to the server.
// In this case, it is to our advantage to read as many as we can before replying
// to the client. The form of a pipelined get is a series of GETQ headers, followed
// by a GET or a NOOP. Once all the headers are sent, the client will start to read
// data sent by the server.
//
// A further optimization would be to return a channel of keys so the retrival can
// get started right away.
//
// https://github.com/couchbase/spymemcached/blob/master/src/main/java/net/spy/memcached/protocol/binary/MultiGetOperationImpl.java#L88
// spymemcached's implementation ^^^

func (b BinaryParser) Parse() (common.Request, common.RequestType, error) {
	// read in the full header before any variable length fields
	reqHeader, err := readRequestHeader(b.reader)
	defer reqHeadPool.Put(reqHeader)

	if err != nil {
		return nil, common.RequestUnknown, err
	}

	switch reqHeader.Opcode {
	case OpcodeSet:
		return setRequest(b.reader, reqHeader, common.RequestSet, false)
	case OpcodeSetQ:
		return setRequest(b.reader, reqHeader, common.RequestSet, true)

	case OpcodeAdd:
		return setRequest(b.reader, reqHeader, common.RequestAdd, false)
	case OpcodeAddQ:
		return setRequest(b.reader, reqHeader, common.RequestAdd, true)

	case OpcodeReplace:
		return setRequest(b.reader, reqHeader, common.RequestReplace, false)
	case OpcodeReplaceQ:
		return setRequest(b.reader, reqHeader, common.RequestReplace, true)

	case OpcodeGetQ:
		req, err := readBatchGet(b.reader, reqHeader)
		if err != nil {
			log.Println("Error reading batch get")
			return nil, common.RequestGet, err
		}

		return req, common.RequestGet, nil

	case OpcodeGet:
		// key
		key, err := readString(b.reader, reqHeader.KeyLength)
		if err != nil {
			log.Println("Error reading key")
			return nil, common.RequestGet, err
		}

		return common.GetRequest{
			Keys:    [][]byte{key},
			Opaques: []uint32{reqHeader.OpaqueToken},
			Quiet:   []bool{false},
			NoopEnd: false,
		}, common.RequestGet, nil

	// Expected only in applications behind Rend that reuse this parsing code
	case OpcodeGetEQ:
		req, err := readBatchGetE(b.reader, reqHeader)
		if err != nil {
			log.Println("Error reading batch get")
			return nil, common.RequestGetE, err
		}

		return req, common.RequestGetE, nil

	// Expected only in applications behind Rend that reuse this parsing code
	case OpcodeGetE:
		// key
		key, err := readString(b.reader, reqHeader.KeyLength)
		if err != nil {
			log.Println("Error reading key")
			return nil, common.RequestGetE, err
		}

		return common.GetRequest{
			Keys:    [][]byte{key},
			Opaques: []uint32{reqHeader.OpaqueToken},
			Quiet:   []bool{false},
			NoopEnd: false,
		}, common.RequestGetE, nil

	case OpcodeGat:
		// exptime, key
		exptime, err := readUInt32(b.reader)
		if err != nil {
			log.Println("Error reading exptime")
			return nil, common.RequestGat, err
		}

		key, err := readString(b.reader, reqHeader.KeyLength)
		if err != nil {
			log.Println("Error reading key")
			return nil, common.RequestGat, err
		}

		return common.GATRequest{
			Key:     key,
			Exptime: exptime,
			Opaque:  reqHeader.OpaqueToken,
		}, common.RequestGat, nil

	case OpcodeDelete:
		// key
		key, err := readString(b.reader, reqHeader.KeyLength)
		if err != nil {
			log.Println("Error reading key")
			return nil, common.RequestDelete, err
		}

		return common.DeleteRequest{
			Key:    key,
			Opaque: reqHeader.OpaqueToken,
		}, common.RequestDelete, nil

	case OpcodeTouch:
		// exptime, key
		exptime, err := readUInt32(b.reader)
		if err != nil {
			log.Println("Error reading exptime")
			return nil, common.RequestTouch, err
		}

		key, err := readString(b.reader, reqHeader.KeyLength)
		if err != nil {
			log.Println("Error reading key")
			return nil, common.RequestTouch, err
		}

		return common.TouchRequest{
			Key:     key,
			Exptime: exptime,
			Opaque:  reqHeader.OpaqueToken,
		}, common.RequestTouch, nil

	case OpcodeNoop:
		return common.NoopRequest{
			Opaque: reqHeader.OpaqueToken,
		}, common.RequestNoop, nil

	case OpcodeQuit:
		return common.QuitRequest{
			Opaque: reqHeader.OpaqueToken,
			Quiet:  false,
		}, common.RequestQuit, nil
	case OpcodeQuitQ:
		return common.QuitRequest{
			Opaque: reqHeader.OpaqueToken,
			Quiet:  true,
		}, common.RequestQuit, nil

	case OpcodeVersion:
		return common.VersionRequest{
			Opaque: reqHeader.OpaqueToken,
		}, common.RequestVersion, nil
	}

	log.Printf("Error processing request: unknown command. Command: %X\nWhole request:%#v", reqHeader.Opcode, reqHeader)

	return nil, common.RequestUnknown, common.ErrUnknownCmd
}

func readBatchGet(r io.Reader, header RequestHeader) (common.GetRequest, error) {
	keys := make([][]byte, 0)
	opaques := make([]uint32, 0)
	quiet := make([]bool, 0)
	var noopOpaque uint32
	var noopEnd bool

	// while GETQ
	// read key, read header
	for header.Opcode == OpcodeGetQ {
		// key
		key, err := readString(r, header.KeyLength)
		if err != nil {
			return common.GetRequest{}, err
		}

		keys = append(keys, key)
		opaques = append(opaques, header.OpaqueToken)
		quiet = append(quiet, true)

		// read in the next header
		reqHeadPool.Put(header)
		header, err = readRequestHeader(r)
		if err != nil {
			return common.GetRequest{}, err
		}
	}

	if header.Opcode == OpcodeGet {
		// key
		key, err := readString(r, header.KeyLength)
		if err != nil {
			return common.GetRequest{}, err
		}

		keys = append(keys, key)
		opaques = append(opaques, header.OpaqueToken)
		quiet = append(quiet, false)
		noopEnd = false

	} else if header.Opcode == OpcodeNoop {
		// nothing to do, header is read already
		noopEnd = true
		noopOpaque = header.OpaqueToken

	} else {
		// no idea... this is a problem though.
		// unexpected patterns shouldn't come over the wire, so maybe it will
		// be OK to simply discount this situation. Probably not.
	}

	// Regardless of the header, we want to put it back here
	reqHeadPool.Put(header)

	return common.GetRequest{
		Keys:       keys,
		Opaques:    opaques,
		Quiet:      quiet,
		NoopOpaque: noopOpaque,
		NoopEnd:    noopEnd,
	}, nil
}

func readBatchGetE(r io.Reader, header RequestHeader) (common.GetRequest, error) {
	keys := make([][]byte, 0)
	opaques := make([]uint32, 0)
	quiet := make([]bool, 0)
	var noopOpaque uint32
	var noopEnd bool

	// while GETQ
	// read key, read header
	for header.Opcode == OpcodeGetEQ {
		// key
		key, err := readString(r, header.KeyLength)
		if err != nil {
			return common.GetRequest{}, err
		}

		keys = append(keys, key)
		opaques = append(opaques, header.OpaqueToken)
		quiet = append(quiet, true)

		// read in the next header
		reqHeadPool.Put(header)
		header, err = readRequestHeader(r)
		if err != nil {
			return common.GetRequest{}, err
		}
	}

	if header.Opcode == OpcodeGetE {
		// key
		key, err := readString(r, header.KeyLength)
		if err != nil {
			return common.GetRequest{}, err
		}

		keys = append(keys, key)
		opaques = append(opaques, header.OpaqueToken)
		quiet = append(quiet, false)
		noopEnd = false

	} else if header.Opcode == OpcodeNoop {
		// nothing to do, header is read already
		noopEnd = true
		noopOpaque = header.OpaqueToken

	} else {
		// no idea... this is a problem though.
		// unexpected patterns shouldn't come over the wire, so maybe it will
		// be OK to simply discount this situation. Probably not.
	}

	// Regardless of the header, we want to put it back here
	reqHeadPool.Put(header)

	return common.GetRequest{
		Keys:       keys,
		Opaques:    opaques,
		Quiet:      quiet,
		NoopOpaque: noopOpaque,
		NoopEnd:    noopEnd,
	}, nil
}

func setRequest(r io.Reader, reqHeader RequestHeader, reqType common.RequestType, quiet bool) (common.SetRequest, common.RequestType, error) {
	// flags, exptime, key, value
	flags, err := readUInt32(r)
	if err != nil {
		log.Println("Error reading flags")
		return common.SetRequest{}, reqType, err
	}

	exptime, err := readUInt32(r)
	if err != nil {
		log.Println("Error reading exptime")
		return common.SetRequest{}, reqType, err
	}

	key, err := readString(r, reqHeader.KeyLength)
	if err != nil {
		log.Println("Error reading key")
		return common.SetRequest{}, reqType, err
	}

	realLength := reqHeader.TotalBodyLength -
		uint32(reqHeader.ExtraLength) -
		uint32(reqHeader.KeyLength)

	// Read in the body of the set request
	dataBuf := make([]byte, realLength)
	n, err := io.ReadAtLeast(r, dataBuf, int(realLength))
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(n))
	if err != nil {
		return common.SetRequest{}, reqType, err
	}

	return common.SetRequest{
		Quiet:   quiet,
		Key:     key,
		Flags:   flags,
		Exptime: exptime,
		Opaque:  reqHeader.OpaqueToken,
		Data:    dataBuf,
	}, reqType, nil
}

func readString(r io.Reader, l uint16) ([]byte, error) {
	buf := make([]byte, l)
	n, err := io.ReadAtLeast(r, buf, int(l))
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(n))
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func readUInt32(r io.Reader) (uint32, error) {
	buf := make([]byte, 4)

	n, err := io.ReadAtLeast(r, buf, 4)
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(n))
	if err != nil {
		return uint32(0), err
	}

	return binary.BigEndian.Uint32(buf), nil
}
