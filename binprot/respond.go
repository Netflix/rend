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

import "bufio"
import "bytes"
import "encoding/binary"

import "github.com/netflix/rend/common"

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
func (b BinaryResponder) Set() error {
	header := makeSuccessResponseHeader(OpcodeSet, 0, 0, 0, 0)
	return writeHeader(header, b.writer)
}

func (b BinaryResponder) Get(response common.GetResponse) error {
	return getCommon(b.writer, response, OpcodeGet)
}

func (b BinaryResponder) GetMiss(response common.GetResponse) error {
	if !response.Quiet {
		header := makeErrorResponseHeader(OpcodeGet, int(StatusKeyEnoent), 0)
		return writeHeader(header, b.writer)
	}
	return nil
}

func (b BinaryResponder) GetEnd(noopEnd bool) error {
	// if Noop was the end of the pipelined batch gets, respond with a Noop header
	// otherwise, stay quiet as the last get would be a GET and not a GETQ
	if noopEnd {
		header := makeSuccessResponseHeader(OpcodeNoop, 0, 0, 0, 0)
		return writeHeader(header, b.writer)
	}

	return nil
}

func (b BinaryResponder) GAT(response common.GetResponse) error {
	return getCommon(b.writer, response, OpcodeGat)
}

func (b BinaryResponder) GATMiss(response common.GetResponse) error {
	if !response.Quiet {
		header := makeErrorResponseHeader(OpcodeGat, int(StatusKeyEnoent), 0)
		return writeHeader(header, b.writer)
	}
	return nil
}

func (b BinaryResponder) Delete() error {
	header := makeSuccessResponseHeader(OpcodeDelete, 0, 0, 0, 0)
	return writeHeader(header, b.writer)
}

func (b BinaryResponder) Touch() error {
	header := makeSuccessResponseHeader(OpcodeTouch, 0, 0, 0, 0)
	return writeHeader(header, b.writer)
}

func (b BinaryResponder) Error(err error) error {
	// TODO: proper opcode
	header := makeErrorResponseHeader(OpcodeGet, int(errorToCode(err)), 0)
	return writeHeader(header, b.writer)
}

func writeHeader(header ResponseHeader, remoteWriter *bufio.Writer) error {
	headerBuf := new(bytes.Buffer)
	binary.Write(headerBuf, binary.BigEndian, header)

	// TODO: Error handling for less bytes
	_, err := remoteWriter.Write(headerBuf.Bytes())
	if err != nil {
		return err
	}

	err = remoteWriter.Flush()
	if err != nil {
		return err
	}

	return nil
}

func getCommon(w *bufio.Writer, response common.GetResponse, opcode int) error {
	// total body length = extras (flags, 4 bytes) + data length
	totalBodyLength := len(response.Data) + 4
	header := makeSuccessResponseHeader(opcode, 0, 4, totalBodyLength, 0)

	err := writeHeader(header, w)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, response.Metadata.OrigFlags)
	if err != nil {
		return err
	}

	_, err = w.Write(response.Data)
	if err != nil {
		return err
	}

	w.Flush()
	return nil
}
