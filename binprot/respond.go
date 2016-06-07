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

// Sample GetE response
// Field        (offset) (value)
//     Magic        (0)    : 0x81
//     Opcode       (1)    : 0x00
//     Key length   (2,3)  : 0x0000
//     Extra length (4)    : 0x08
//     Data type    (5)    : 0x00
//     Status       (6,7)  : 0x0000
//     Total body   (8-11) : 0x00000009
//     Opaque       (12-15): 0x00000000
//     CAS          (16-23): 0x00000000000001234
//     Extras              :
//       Flags      (24-27): 0xdeadbeef
//       Exptime    (28-31): 0xcafebabe
//     Key                 : None
//     Value        (32-36): The textual string "World"

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

func (b BinaryResponder) Set(opaque uint32, quiet bool) error {
	if !quiet {
		return writeSuccessResponseHeader(b.writer, OpcodeSet, 0, 0, 0, opaque, true)
	}
	return nil
}

func (b BinaryResponder) Add(opaque uint32, quiet bool) error {
	if !quiet {
		return writeSuccessResponseHeader(b.writer, OpcodeAdd, 0, 0, 0, opaque, true)
	}
	return nil
}

func (b BinaryResponder) Replace(opaque uint32, quiet bool) error {
	if !quiet {
		return writeSuccessResponseHeader(b.writer, OpcodeReplace, 0, 0, 0, opaque, true)
	}
	return nil
}

func (b BinaryResponder) Append(opaque uint32, quiet bool) error {
	if !quiet {
		return writeSuccessResponseHeader(b.writer, OpcodeAppend, 0, 0, 0, opaque, true)
	}
	return nil
}

func (b BinaryResponder) Prepend(opaque uint32, quiet bool) error {
	if !quiet {
		return writeSuccessResponseHeader(b.writer, OpcodePrepend, 0, 0, 0, opaque, true)
	}
	return nil
}

func (b BinaryResponder) Get(response common.GetResponse) error {
	if response.Miss {
		if !response.Quiet {
			return b.Error(response.Opaque, common.RequestGet, common.ErrKeyNotFound, false)
		}
		return nil
	}

	return getCommon(b.writer, response, OpcodeGet)
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
	if response.Miss {
		if !response.Quiet {
			return b.Error(response.Opaque, common.RequestGat, common.ErrKeyNotFound, false)
		}
		return nil
	}

	return getCommon(b.writer, response, OpcodeGat)
}

func (b BinaryResponder) GetE(response common.GetEResponse) error {
	if response.Miss {
		if !response.Quiet {
			return b.Error(response.Opaque, common.RequestGetE, common.ErrKeyNotFound, false)
		}
		return nil
	}

	// total body length = extras (flags & exptime, 8 bytes) + data length
	totalBodyLength := len(response.Data) + 8
	writeSuccessResponseHeader(b.writer, common.RequestGetE, 0, 8, totalBodyLength, response.Opaque, false)
	binary.Write(b.writer, binary.BigEndian, response.Flags)
	binary.Write(b.writer, binary.BigEndian, response.Exptime)
	b.writer.Write(response.Data)

	if err := b.writer.Flush(); err != nil {
		return err
	}
	metrics.IncCounterBy(common.MetricBytesWrittenRemote, uint64(totalBodyLength))
	return nil
}

func (b BinaryResponder) Delete(opaque uint32) error {
	return writeSuccessResponseHeader(b.writer, OpcodeDelete, 0, 0, 0, opaque, true)
}

func (b BinaryResponder) Touch(opaque uint32) error {
	return writeSuccessResponseHeader(b.writer, OpcodeTouch, 0, 0, 0, opaque, true)
}

func (b BinaryResponder) Noop(opaque uint32) error {
	return writeSuccessResponseHeader(b.writer, OpcodeNoop, 0, 0, 0, opaque, true)
}

func (b BinaryResponder) Quit(opaque uint32, quiet bool) error {
	if !quiet {
		return writeSuccessResponseHeader(b.writer, OpcodeQuit, 0, 0, 0, opaque, true)
	}
	return nil
}

func (b BinaryResponder) Version(opaque uint32) error {
	if err := writeSuccessResponseHeader(b.writer, OpcodeVersion, 0, 0, len(common.VersionString), opaque, false); err != nil {
		return err
	}
	n, _ := b.writer.WriteString(common.VersionString)
	metrics.IncCounterBy(common.MetricBytesWrittenRemote, uint64(n))
	return b.writer.Flush()
}

func (b BinaryResponder) Error(opaque uint32, reqType common.RequestType, err error, quiet bool) error {
	// TODO: proper opcode
	return writeErrorResponseHeader(b.writer, reqTypeToOpcode(reqType, quiet), errorToCode(err), opaque)
}

// Mae sure this includes all possibilities in the github.com/netflix/rend/common.RequestType enum
func reqTypeToOpcode(rt common.RequestType, quiet bool) uint8 {
	switch {
	case rt == common.RequestGet && quiet:
		return OpcodeGet
	case rt == common.RequestGet && !quiet:
		return OpcodeGet
	case rt == common.RequestGat:
		return OpcodeGat
	case rt == common.RequestGetE:
		return OpcodeGetE
	case rt == common.RequestSet && quiet:
		return OpcodeSetQ
	case rt == common.RequestSet && !quiet:
		return OpcodeSet
	case rt == common.RequestAdd && quiet:
		return OpcodeAddQ
	case rt == common.RequestAdd && !quiet:
		return OpcodeAdd
	case rt == common.RequestReplace && quiet:
		return OpcodeReplaceQ
	case rt == common.RequestReplace && !quiet:
		return OpcodeReplace
	case rt == common.RequestAppend && quiet:
		return OpcodeAppendQ
	case rt == common.RequestAppend && !quiet:
		return OpcodeAppend
	case rt == common.RequestPrepend && quiet:
		return OpcodePrependQ
	case rt == common.RequestPrepend && !quiet:
		return OpcodePrepend
	case rt == common.RequestDelete && quiet:
		return OpcodeDeleteQ
	case rt == common.RequestDelete && !quiet:
		return OpcodeDelete
	case rt == common.RequestTouch:
		return OpcodeTouch
	default:
		return OpcodeInvalid
	}
}

func getCommon(w *bufio.Writer, response common.GetResponse, opcode uint8) error {
	// total body length = extras (flags, 4 bytes) + data length
	totalBodyLength := len(response.Data) + 4
	writeSuccessResponseHeader(w, opcode, 0, 4, totalBodyLength, response.Opaque, false)
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, response.Flags)
	w.Write(buf)
	w.Write(response.Data)
	if err := w.Flush(); err != nil {
		return err
	}
	metrics.IncCounterBy(common.MetricBytesWrittenRemote, uint64(totalBodyLength))
	return nil
}

func writeSuccessResponseHeader(w *bufio.Writer, opcode uint8, keyLength, extraLength,
	totalBodyLength int, opaque uint32, flush bool) error {

	header := resHeadPool.Get().(ResponseHeader)

	header.Magic = MagicResponse
	header.Opcode = opcode
	header.KeyLength = uint16(keyLength)
	header.ExtraLength = uint8(extraLength)
	header.DataType = uint8(0)
	header.Status = StatusSuccess
	header.TotalBodyLength = uint32(totalBodyLength)
	header.OpaqueToken = opaque
	header.CASToken = uint64(0)

	if err := writeResponseHeader(w, header); err != nil {
		resHeadPool.Put(header)
		return err
	}

	if flush {
		if err := w.Flush(); err != nil {
			resHeadPool.Put(header)
			return err
		}
	}

	metrics.IncCounterBy(common.MetricBytesWrittenRemote, resHeaderLen)
	resHeadPool.Put(header)

	return nil
}

func writeErrorResponseHeader(w *bufio.Writer, opcode uint8, status uint16, opaque uint32) error {
	header := resHeadPool.Get().(ResponseHeader)

	header.Magic = MagicResponse
	header.Opcode = opcode
	header.KeyLength = uint16(0)
	header.ExtraLength = uint8(0)
	header.DataType = uint8(0)
	header.Status = status
	header.TotalBodyLength = uint32(0)
	header.OpaqueToken = opaque
	header.CASToken = uint64(0)

	if err := writeResponseHeader(w, header); err != nil {
		resHeadPool.Put(header)
		return err
	}

	if err := w.Flush(); err != nil {
		resHeadPool.Put(header)
		return err
	}

	metrics.IncCounterBy(common.MetricBytesWrittenRemote, resHeaderLen)
	resHeadPool.Put(header)

	return nil
}
