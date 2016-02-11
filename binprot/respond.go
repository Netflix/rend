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

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
)

// Sample Get response
// Field        (offset) (value)
//     Magic        (0)    : 0x81
//     Opcode       (1)    : 0x00
//     Key length   (2,3)  : 0x0000
//     Extra length (4)    : 0x04
//     Data type    (5)    : 0x00
//     Status       (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000009
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x00000000000001234
//     Extras              :
//       Flags      (24-27): 0xdeadbeef
//     Key                 : None
//     Value        (28-32): The textual string "World"

// Sample GAT response
// Field        (offset) (value)
//     Magic        (0)    : 0x81
//     Opcode       (1)    : 0x1d
//     Key length   (2,3)  : 0x0000
//     Extra length (4)    : 0x04
//     Data type    (5)    : 0x00
//     Status       (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000009
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x00000000000001234
//     Extras              :
//       Flags      (24-27): 0xdeadbeef
//     Key                 : None
//     Value        (28-32): The textual string "World"

// Sample Set response
// Field        (offset) (value)
//     Magic        (0)    : 0x81
//     Opcode       (1)    : 0x02
//     Key length   (2,3)  : 0x0000
//     Extra length (4)    : 0x00
//     Data type    (5)    : 0x00
//     Status       (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000000
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000001234
//     Extras              : None
//     Key                 : None
//     Value               : None

// Sample Delete response
// Field        (offset) (value)
//     Magic        (0)    : 0x81
//     Opcode       (1)    : 0x04
//     Key length   (2,3)  : 0x0000
//     Extra length (4)    : 0x00
//     Data type    (5)    : 0x00
//     Status       (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000000
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              : None
//     Key                 : None
//     Value               : None

// Sample Touch response
// Field        (offset) (value)
//     Magic        (0)    : 0x81
//     Opcode       (1)    : 0x1c
//     Key length   (2,3)  : 0x0000
//     Extra length (4)    : 0x00
//     Data type    (5)    : 0x00
//     Status       (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000000
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x0000000000000000
//     Extras              : None
//     Key                 : None
//     Value               : None

type BinaryResponder struct {
	writer *bufio.Writer
}

func NewBinaryResponder(writer *bufio.Writer) BinaryResponder {
	return BinaryResponder{
		writer: writer,
	}
}

// TODO: CAS?
func (b BinaryResponder) Set(opaque uint32) error {
	return writeSuccessResponseHeader(b.writer, OpcodeSet, 0, 0, 0, opaque, true)
}

func (b BinaryResponder) Get(response common.GetResponse) error {
	return getCommon(b.writer, response, OpcodeGet)
}

func (b BinaryResponder) GetMiss(response common.GetResponse) error {
	if !response.Quiet {
		return b.Error(response.Opaque, common.ErrKeyNotFound)
	}
	return nil
}

func (b BinaryResponder) GetEnd(opaque uint32, noopEnd bool) error {
	// if Noop was the end of the pipelined batch gets, respond with a Noop header
	// otherwise, stay quiet as the last get would be a GET and not a GETQ
	if noopEnd {
		return writeSuccessResponseHeader(b.writer, OpcodeNoop, 0, 0, 0, opaque, true)
	}

	return nil
}

func (b BinaryResponder) GAT(response common.GetResponse) error {
	return getCommon(b.writer, response, OpcodeGat)
}

func (b BinaryResponder) GATMiss(response common.GetResponse) error {
	if !response.Quiet {
		return b.Error(response.Opaque, common.ErrKeyNotFound)
	}
	return nil
}

func (b BinaryResponder) Delete(opaque uint32) error {
	return writeSuccessResponseHeader(b.writer, OpcodeDelete, 0, 0, 0, opaque, true)
}

func (b BinaryResponder) Touch(opaque uint32) error {
	return writeSuccessResponseHeader(b.writer, OpcodeTouch, 0, 0, 0, opaque, true)
}

func (b BinaryResponder) Error(opaque uint32, err error) error {
	// TODO: proper opcode
	return writeErrorResponseHeader(b.writer, OpcodeGet, errorToCode(err), opaque)
}

func getCommon(w *bufio.Writer, response common.GetResponse, opcode uint8) error {
	// total body length = extras (flags, 4 bytes) + data length
	totalBodyLength := len(response.Data) + 4
	if err := writeSuccessResponseHeader(w, opcode, 0, 4, totalBodyLength, response.Opaque, false); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, response.Flags); err != nil {
		return err
	}
	if _, err := w.Write(response.Data); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	metrics.IncCounterBy(common.MetricBytesWrittenRemote, uint64(totalBodyLength))
	return nil
}

func writeSuccessResponseHeader(w *bufio.Writer, opcode uint8, keyLength, extraLength,
	totalBodyLength int, opaque uint32, flush bool) error {

	header := ResponseHeader{
		Magic:           MagicResponse,
		Opcode:          uint8(opcode),
		KeyLength:       uint16(keyLength),
		ExtraLength:     uint8(extraLength),
		DataType:        uint8(0),
		Status:          uint16(StatusSuccess),
		TotalBodyLength: uint32(totalBodyLength),
		OpaqueToken:     opaque,
		CASToken:        uint64(0),
	}

	if err := binary.Write(w, binary.BigEndian, header); err != nil {
		return err
	}

	if flush {
		if err := w.Flush(); err != nil {
			return err
		}
	}

	metrics.IncCounterBy(common.MetricBytesWrittenRemote, resHeaderLen)

	return nil
}

func writeErrorResponseHeader(w *bufio.Writer, opcode uint8, status uint16, opaque uint32) error {
	header := ResponseHeader{
		Magic:           MagicResponse,
		Opcode:          opcode,
		KeyLength:       uint16(0),
		ExtraLength:     uint8(0),
		DataType:        uint8(0),
		Status:          status,
		TotalBodyLength: uint32(0),
		OpaqueToken:     opaque,
		CASToken:        uint64(0),
	}

	if err := binary.Write(w, binary.BigEndian, header); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	metrics.IncCounterBy(common.MetricBytesWrittenRemote, resHeaderLen)

	return nil
}
