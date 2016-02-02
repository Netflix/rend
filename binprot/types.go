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
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/netflix/rend/common"
)

var ErrBadMagic = errors.New("Bad magic value")

const (
	MagicRequest  = 0x80
	MagicResponse = 0x81

	// All opcodes as defined in memcached
	// Minus SASL and range ops
	OpcodeGet        = 0x00
	OpcodeSet        = 0x01
	OpcodeAdd        = 0x02
	OpcodeReplace    = 0x03
	OpcodeDelete     = 0x04
	OpcodeIncrement  = 0x05
	OpcodeDecrement  = 0x06
	OpcodeQuit       = 0x07
	OpcodeFlush      = 0x08
	OpcodeGetQ       = 0x09
	OpcodeNoop       = 0x0a
	OpcodeVersion    = 0x0b
	OpcodeGetK       = 0x0c
	OpcodeGetKQ      = 0x0d
	OpcodeAppend     = 0x0e
	OpcodePrepend    = 0x0f
	OpcodeStat       = 0x10
	OpcodeSetQ       = 0x11
	OpcodeAddQ       = 0x12
	OpcodeReplaceQ   = 0x13
	OpcodeDeleteQ    = 0x14
	OpcodeIncrementQ = 0x15
	OpcodeDecrementQ = 0x16
	OpcodeQuitQ      = 0x17
	OpcodeFlushQ     = 0x18
	OpcodeAppendQ    = 0x19
	OpcodePrependQ   = 0x1a
	OpcodeTouch      = 0x1c
	OpcodeGat        = 0x1d
	OpcodeGatQ       = 0x1e
	OpcodeGatK       = 0x23
	OpcodeGatKQ      = 0x24
	OpcodeInvalid    = 0xFF

	StatusSuccess        = uint16(0x00)
	StatusKeyEnoent      = uint16(0x01)
	StatusKeyEexists     = uint16(0x02)
	StatusE2big          = uint16(0x03)
	StatusEinval         = uint16(0x04)
	StatusNotStored      = uint16(0x05)
	StatusDeltaBadval    = uint16(0x06)
	StatusAuthError      = uint16(0x20)
	StatusAuthContinue   = uint16(0x21)
	StatusUnknownCommand = uint16(0x81)
	StatusEnomem         = uint16(0x82)
	StatusNotSupported   = uint16(0x83)
	StatusInternalError  = uint16(0x84)
	StatusBusy           = uint16(0x85)
	StatusTempFailure    = uint16(0x86)
	StatusInvalid        = uint16(0xFFFF)
)

func DecodeError(header ResponseHeader) error {
	switch header.Status {
	case StatusKeyEnoent:
		return common.ErrKeyNotFound
	case StatusKeyEexists:
		return common.ErrKeyExists
	case StatusE2big:
		return common.ErrValueTooBig
	case StatusEinval:
		return common.ErrInvalidArgs
	case StatusNotStored:
		return common.ErrItemNotStored
	case StatusDeltaBadval:
		return common.ErrBadIncDecValue
	case StatusAuthError:
		return common.ErrAuth
	case StatusUnknownCommand:
		return common.ErrUnknownCmd
	case StatusEnomem:
		return common.ErrNoMem
	case StatusNotSupported:
		return common.ErrNotSupported
	case StatusInternalError:
		return common.ErrInternal
	case StatusBusy:
		return common.ErrBusy
	case StatusTempFailure:
		return common.ErrTempFailure
	}
	return nil
}

func errorToCode(err error) uint16 {
	switch err {
	case common.ErrKeyNotFound:
		return StatusKeyEnoent
	case common.ErrKeyExists:
		return StatusKeyEexists
	case common.ErrValueTooBig:
		return StatusE2big
	case common.ErrInvalidArgs:
		return StatusEinval
	case common.ErrItemNotStored:
		return StatusNotStored
	case common.ErrBadIncDecValue:
		return StatusDeltaBadval
	case common.ErrAuth:
		return StatusAuthError
	case common.ErrUnknownCmd:
		return StatusUnknownCommand
	case common.ErrNoMem:
		return StatusEnomem
	case common.ErrNotSupported:
		return StatusNotSupported
	case common.ErrInternal:
		return StatusInternalError
	case common.ErrBusy:
		return StatusBusy
	case common.ErrTempFailure:
		return StatusTempFailure
	}
	return StatusInvalid
}

const ReqHeaderLen = 24

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
	headerBuf := make([]byte, ReqHeaderLen)
	if _, err := io.ReadFull(reader, headerBuf); err != nil {
		return RequestHeader{}, err
	}

	var reqHeader RequestHeader
	binary.Read(bytes.NewBuffer(headerBuf), binary.BigEndian, &reqHeader)

	if reqHeader.Magic != MagicRequest {
		return RequestHeader{}, ErrBadMagic
	}

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
	if _, err := io.ReadFull(reader, headerBuf); err != nil {
		return ResponseHeader{}, err
	}

	var resHeader ResponseHeader
	binary.Read(bytes.NewBuffer(headerBuf), binary.BigEndian, &resHeader)

	if resHeader.Magic != MagicResponse {
		return ResponseHeader{}, ErrBadMagic
	}

	return resHeader, nil
}
