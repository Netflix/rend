/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package binprot

import "bufio"
import "encoding/binary"
import "fmt"
import "io"

import "../common"

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

func (b BinaryParser) Parse() (interface{}, common.RequestType, error) {
	// read in the full header before any variable length fields
	var reqHeader RequestHeader
	err := binary.Read(b.reader, binary.BigEndian, &reqHeader)

	if err != nil {
		return nil, common.REQUEST_UNKNOWN, err
	}

	switch reqHeader.Opcode {
	case OPCODE_SET:
		// flags, exptime, key, value
		flags, err := readUInt32(b.reader)

		if err != nil {
			fmt.Println("Error reading flags")
			return nil, common.REQUEST_SET, err
		}

		exptime, err := readUInt32(b.reader)

		if err != nil {
			fmt.Println("Error reading exptime")
			return nil, common.REQUEST_SET, err
		}

		key, err := readString(b.reader, reqHeader.KeyLength)

		if err != nil {
			fmt.Println("Error reading key")
			return nil, common.REQUEST_SET, err
		}

		realLength := reqHeader.TotalBodyLength -
			uint32(reqHeader.ExtraLength) -
			uint32(reqHeader.KeyLength)

		return common.SetRequest{
			Key:     key,
			Flags:   flags,
			Exptime: exptime,
			Length:  realLength,
		}, common.REQUEST_SET, nil

	case OPCODE_GETQ:
		req, err := readBatchGet(b.reader, reqHeader)

		if err != nil {
			fmt.Println("Error reading batch get")
			return nil, common.REQUEST_GET, err
		}

		return req, common.REQUEST_GET, nil

	case OPCODE_GET:
		// key
		key, err := readString(b.reader, reqHeader.KeyLength)

		if err != nil {
			fmt.Println("Error reading key")
			return nil, common.REQUEST_GET, err
		}

		return common.GetRequest{
			Keys:    [][]byte{key},
			Opaques: []uint32{reqHeader.OpaqueToken},
			Quiet:   []bool{false},
			NoopEnd: false,
		}, common.REQUEST_GET, nil

	case OPCODE_DELETE:
		// key
		key, err := readString(b.reader, reqHeader.KeyLength)

		if err != nil {
			fmt.Println("Error reading key")
			return nil, common.REQUEST_DELETE, err
		}

		return common.DeleteRequest{
			Key: key,
		}, common.REQUEST_DELETE, nil

	case OPCODE_TOUCH:
		// exptime, key
		exptime, err := readUInt32(b.reader)

		if err != nil {
			fmt.Println("Error reading exptime")
			return nil, common.REQUEST_TOUCH, err
		}

		key, err := readString(b.reader, reqHeader.KeyLength)

		if err != nil {
			fmt.Println("Error reading key")
			return nil, common.REQUEST_TOUCH, err
		}

		return common.TouchRequest{
			Key:     key,
			Exptime: exptime,
		}, common.REQUEST_TOUCH, nil
	}

	return nil, common.REQUEST_GET, nil
}

func readBatchGet(r io.Reader, header RequestHeader) (common.GetRequest, error) {
	keys := make([][]byte, 0)
	opaques := make([]uint32, 0)
	quiet := make([]bool, 0)
	var noopEnd bool

	// while GETQ
	// read key, read header
	for header.Opcode == OPCODE_GETQ {
		// key
		key, err := readString(r, header.KeyLength)

		if err != nil {
			return common.GetRequest{}, err
		}

		keys = append(keys, key)
		opaques = append(opaques, header.OpaqueToken)
		quiet = append(quiet, true)

		// read in the next header
		err = binary.Read(r, binary.BigEndian, &header)

		if err != nil {
			return common.GetRequest{}, err
		}
	}

	if header.Opcode == OPCODE_GET {
		// key
		key, err := readString(r, header.KeyLength)

		if err != nil {
			return common.GetRequest{}, err
		}

		keys = append(keys, key)
		opaques = append(opaques, header.OpaqueToken)
		quiet = append(quiet, false)
		noopEnd = false
	} else if header.Opcode == OPCODE_NOOP {
		// nothing to do, header is read already
		noopEnd = true
	} else {
		// no idea... this is a problem though.
		// unexpected patterns shouldn't come over the wire, so maybe it will
		// be OK to simply discount this situation. Probably not.
	}

	return common.GetRequest{
		Keys:    keys,
		Opaques: opaques,
		Quiet:   quiet,
		NoopEnd: noopEnd,
	}, nil
}

func readString(r io.Reader, l uint16) ([]byte, error) {
	buf := make([]byte, l)
	_, err := io.ReadFull(r, buf)

	if err != nil {
		return nil, err
	}

	return buf, nil
}

func readUInt32(r io.Reader) (uint32, error) {
	var num uint32
	err := binary.Read(r, binary.BigEndian, &num)

	if err != nil {
		return uint32(0), err
	}

	return num, nil
}
