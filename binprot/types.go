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

import "bytes"
import "encoding/binary"
import "io"

import "github.com/netflix/rend/common"

const (
	MagicRequest = 0x80
	MagicResponse = 0x81

	// All opcodes as defined in memcached
	// Minus SASL and range ops
	OpcodeGet = 0x00
	OpcodeSet = 0x01
	OpcodeAdd = 0x02
	OpcodeReplace = 0x03
	OpcodeDelete = 0x04
	OpcodeIncrement = 0x05
	OpcodeDecrement = 0x06
	OpcodeQuit = 0x07
	OpcodeFlush = 0x08
	OpcodeGetQ = 0x09
	OpcodeNoop = 0x0a
	OpcodeVersion = 0x0b
	OpcodeGetK = 0x0c
	OpcodeGetKQ = 0x0d
	OpcodeAppend = 0x0e
	OpcodePrepend = 0x0f
	OpcodeStat = 0x10
	OpcodeSetQ = 0x11
	OpcodeAddQ = 0x12
	OpcodeReplaceQ = 0x13
	OpcodeDeleteQ = 0x14
	OpcodeIncrementQ = 0x15
	OpcodeDecrementQ = 0x16
	OpcodeQuitQ = 0x17
	OpcodeFlushQ = 0x18
	OpcodeAppendQ = 0x19
	OpcodePrependQ = 0x1a
	OpcodeTouch = 0x1c
	OpcodeGat = 0x1d
	OpcodeGatQ = 0x1e
	OpcodeGatK = 0x23
	OpcodeGatKQ = 0x24


	StatusSuccess = uint16(0x00)
	StatusKeyEnoent = uint16(0x01)
	StatusKeyEexists = uint16(0x02)
	StatusE2big = uint16(0x03)
	StatusEinval = uint16(0x04)
	StatusNotStored = uint16(0x05)
	StatusDeltaBadval = uint16(0x06)
	StatusAuthError = uint16(0x20)
	StatusAuthContinue = uint16(0x21)
	StatusUnknownCommand = uint16(0x81)
	StatusEnomem = uint16(0x82)
)

func DecodeError(header ResponseHeader) error {
	switch header.Status {
	case StatusKeyEnoent:
		return common.ERROR_KEY_NOT_FOUND
	case StatusKeyEexists:
		return common.ERROR_KEY_EXISTS
	case StatusE2big:
		return common.ERROR_VALUE_TOO_BIG
	case StatusEinval:
		return common.ERROR_INVALID_ARGS
	case StatusNotStored:
		return common.ERROR_ITEM_NOT_STORED
	case StatusDeltaBadval:
		return common.ERROR_BAD_INC_DEC_VALUE
	case StatusAuthError:
		return common.ERROR_AUTH_ERROR
	case StatusUnknownCommand:
		return common.ERROR_UNKNOWN_CMD
	case StatusEnomem:
		return common.ERROR_NO_MEM
	}
	return nil
}

func errorToCode(err error) uint16 {
	switch err {
	case common.ERROR_KEY_NOT_FOUND:
		return StatusKeyEnoent
	case common.ERROR_KEY_EXISTS:
		return StatusKeyEexists
	case common.ERROR_VALUE_TOO_BIG:
		return StatusE2big
	case common.ERROR_INVALID_ARGS:
		return StatusEinval
	case common.ERROR_ITEM_NOT_STORED:
		return StatusNotStored
	case common.ERROR_BAD_INC_DEC_VALUE:
		return StatusDeltaBadval
	case common.ERROR_AUTH_ERROR:
		return StatusAuthError
	case common.ERROR_UNKNOWN_CMD:
		return StatusUnknownCommand
	case common.ERROR_NO_MEM:
		return StatusEnomem
	}
	return 0xFFFF
}

const reqHeaderLen = 24

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

func MakeRequestHeader(opcode, keyLength, extraLength, totalBodyLength int) RequestHeader {
	return RequestHeader{
		Magic:           uint8(MagicRequest),
		Opcode:          uint8(opcode),
		KeyLength:       uint16(keyLength),
		ExtraLength:     uint8(extraLength),
		DataType:        uint8(0),
		VBucket:         uint16(0),
		TotalBodyLength: uint32(totalBodyLength),
		OpaqueToken:     uint32(0),
		CASToken:        uint64(0),
	}
}

func ReadRequestHeader(reader io.Reader) (RequestHeader, error) {
	// read in the full header before any variable length fields
	headerBuf := make([]byte, reqHeaderLen)
	_, err := io.ReadFull(reader, headerBuf)

	if err != nil {
		return RequestHeader{}, err
	}

	var reqHeader RequestHeader
	binary.Read(bytes.NewBuffer(headerBuf), binary.BigEndian, &reqHeader)

	return reqHeader, nil
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

func makeSuccessResponseHeader(opcode, keyLength, extraLength, totalBodyLength, opaqueToken int) ResponseHeader {
	return ResponseHeader{
		Magic:           MagicResponse,
		Opcode:          uint8(opcode),
		KeyLength:       uint16(keyLength),
		ExtraLength:     uint8(extraLength),
		DataType:        uint8(0),
		Status:          uint16(StatusSuccess),
		TotalBodyLength: uint32(totalBodyLength),
		OpaqueToken:     uint32(opaqueToken),
		CASToken:        uint64(0),
	}
}

func makeErrorResponseHeader(opcode, status, opaqueToken int) ResponseHeader {
	return ResponseHeader{
		Magic:           MagicResponse,
		Opcode:          uint8(opcode),
		KeyLength:       uint16(0),
		ExtraLength:     uint8(0),
		DataType:        uint8(0),
		Status:          uint16(status),
		TotalBodyLength: uint32(0),
		OpaqueToken:     uint32(opaqueToken),
		CASToken:        uint64(0),
	}
}

func ReadResponseHeader(reader io.Reader) (ResponseHeader, error) {
	// read in the full header before any variable length fields
	headerBuf := make([]byte, resHeaderLen)
	_, err := io.ReadFull(reader, headerBuf)

	if err != nil {
		return ResponseHeader{}, err
	}

	var resHeader ResponseHeader
	binary.Read(bytes.NewBuffer(headerBuf), binary.BigEndian, &resHeader)

	return resHeader, nil
}
