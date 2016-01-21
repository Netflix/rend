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

import "encoding/binary"
import "errors"
import "io"

const Get = 0x00
const Set = 0x01
const Add = 0x02
const Replace = 0x03
const Delete = 0x04
const Increment = 0x05
const Decrement = 0x06
const Quit = 0x07
const Flush = 0x08
const GetQ = 0x09
const Noop = 0x0a
const Version = 0x0b
const GetK = 0x0c
const GetKQ = 0x0d
const Append = 0x0e
const Prepend = 0x0f
const Stat = 0x10
const SetQ = 0x11
const AddQ = 0x12
const ReplaceQ = 0x13
const DeleteQ = 0x14
const IncrementQ = 0x15
const DecrementQ = 0x16
const QuitQ = 0x17
const FlushQ = 0x18
const AppendQ = 0x19
const PrependQ = 0x1a
const Verbosity = 0x1b
const Touch = 0x1c
const GAT = 0x1d
const GATQ = 0x1e

type req struct {
	Magic    uint8
	Opcode   uint8
	KeyLen   uint16
	ExtraLen uint8
	DataType uint8
	VBucket  uint16
	BodyLen  uint32
	Opaque   uint32
	CAS      uint64
}

func writeReq(w io.Writer, opcode int, keylen, extralen, bodylen int) error {
	req := req{
		Magic:    0x80,
		Opcode:   uint8(opcode),
		KeyLen:   uint16(keylen),
		ExtraLen: uint8(extralen),
		DataType: 0,
		VBucket:  0,
		BodyLen:  uint32(bodylen),
		Opaque:   0,
		CAS:      0,
	}

	return binary.Write(w, binary.BigEndian, req)
}

type res struct {
	Magic    uint8
	Opcode   uint8
	KeyLen   uint16
	ExtraLen uint8
	DataType uint8
	Status   uint16
	BodyLen  uint32
	Opaque   uint32
	CAS      uint64
}

func readRes(r io.Reader) (*res, error) {
	res := new(res)
	err := binary.Read(r, binary.BigEndian, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

var ErrKeyNotFound error
var ErrKeyExists error
var ErrValTooLarge error
var ErrInvalidArgs error
var ErrItemNotStored error
var ErrIncDecInval error
var ErrVBucket error
var ErrAuth error
var ErrAuthCont error
var ErrUnknownCmd error
var ErrNoMem error
var ErrNotSupported error
var ErrInternal error
var ErrBusy error
var ErrTemp error

func init() {
	ErrKeyNotFound = errors.New("Key not found")
	ErrKeyExists = errors.New("Key exists")
	ErrValTooLarge = errors.New("Value too large")
	ErrInvalidArgs = errors.New("Invalid arguments")
	ErrItemNotStored = errors.New("Item not stored")
	ErrIncDecInval = errors.New("Incr/Decr on non-numeric value.")
	ErrVBucket = errors.New("The vbucket belongs to another server")
	ErrAuth = errors.New("Authentication error")
	ErrAuthCont = errors.New("Authentication continue")
	ErrUnknownCmd = errors.New("Unknown command")
	ErrNoMem = errors.New("Out of memory")
	ErrNotSupported = errors.New("Not supported")
	ErrInternal = errors.New("Internal error")
	ErrBusy = errors.New("Busy")
	ErrTemp = errors.New("Temporary failure")
}

func statusToError(status uint16) error {
	switch status {
	case uint16(0x01):
		return ErrKeyNotFound
	case uint16(0x02):
		return ErrKeyExists
	case uint16(0x03):
		return ErrValTooLarge
	case uint16(0x04):
		return ErrInvalidArgs
	case uint16(0x05):
		return ErrItemNotStored
	case uint16(0x06):
		return ErrIncDecInval
	case uint16(0x07):
		return ErrVBucket
	case uint16(0x08):
		return ErrAuth
	case uint16(0x09):
		return ErrAuthCont
	case uint16(0x81):
		return ErrUnknownCmd
	case uint16(0x82):
		return ErrNoMem
	case uint16(0x83):
		return ErrNotSupported
	case uint16(0x84):
		return ErrInternal
	case uint16(0x85):
		return ErrBusy
	case uint16(0x86):
		return ErrTemp
	}

	return nil
}

func srsErr(err error) bool {
	switch err {
	case ErrKeyNotFound:
	case ErrKeyExists:
	case ErrItemNotStored:
		return false
	}

	return true
}
