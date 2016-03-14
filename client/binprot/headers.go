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
	"errors"
	"io"
	"sync"
)

const (
	Get        = 0x00
	Set        = 0x01
	Add        = 0x02
	Replace    = 0x03
	Delete     = 0x04
	Increment  = 0x05
	Decrement  = 0x06
	Quit       = 0x07
	Flush      = 0x08
	GetQ       = 0x09
	Noop       = 0x0a
	Version    = 0x0b
	GetK       = 0x0c
	GetKQ      = 0x0d
	Append     = 0x0e
	Prepend    = 0x0f
	Stat       = 0x10
	SetQ       = 0x11
	AddQ       = 0x12
	ReplaceQ   = 0x13
	DeleteQ    = 0x14
	IncrementQ = 0x15
	DecrementQ = 0x16
	QuitQ      = 0x17
	FlushQ     = 0x18
	AppendQ    = 0x19
	PrependQ   = 0x1a
	Verbosity  = 0x1b
	Touch      = 0x1c
	GAT        = 0x1d
	GATQ       = 0x1e
        GetE       = 0x40
)

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

var bufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 24, 24)
	},
}

var resPool = &sync.Pool{
	New: func() interface{} {
		return res{}
	},
}

func writeReq(w io.Writer, opcode, keylen, extralen, bodylen, opaque int) error {
	buf := bufPool.Get().([]byte)

	buf[0] = 0x80
	buf[1] = uint8(opcode)
	buf[2] = uint8(keylen >> 8)
	buf[3] = uint8(keylen)
	buf[4] = uint8(extralen)
	// DataType and VBucket are unused
	buf[5] = 0
	buf[6] = 0
	buf[7] = 0
	buf[8] = uint8(bodylen >> 24)
	buf[9] = uint8(bodylen >> 16)
	buf[10] = uint8(bodylen >> 8)
	buf[11] = uint8(bodylen)
	buf[12] = uint8(opaque >> 24)
	buf[13] = uint8(opaque >> 16)
	buf[14] = uint8(opaque >> 8)
	buf[15] = uint8(opaque)

	// zero CAS region
	for i := 16; i < 24; i++ {
		buf[i] = 0
	}

	_, err := w.Write(buf)
	bufPool.Put(buf)
	return err
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

func readRes(r io.Reader) (res, error) {
	buf := bufPool.Get().([]byte)

	if _, err := io.ReadAtLeast(r, buf, 24); err != nil {
		bufPool.Put(buf)
		return res{}, err
	}

	if buf[0] != 0x81 {
		bufPool.Put(buf)
		return res{}, errors.New("Bad Magic")
	}

	res := resPool.Get().(res)
	res.Magic = buf[0]
	res.Opcode = buf[1]
	res.KeyLen = uint16(buf[2])<<8 | uint16(buf[3])
	res.ExtraLen = buf[4]
	// ignore DataType
	//res.DataType = 0
	res.Status = uint16(buf[6])<<8 | uint16(buf[7])
	res.BodyLen = uint32(buf[8])<<24 | uint32(buf[9])<<16 | uint32(buf[10])<<8 | uint32(buf[11])
	res.Opaque = uint32(buf[12])<<24 | uint32(buf[13])<<16 | uint32(buf[14])<<8 | uint32(buf[15])
	// Ignore CAS
	//res.CASToken = 0

	bufPool.Put(buf)

	return res, nil
}

var (
	ErrKeyNotFound   = errors.New("Key not found")
	ErrKeyExists     = errors.New("Key exists")
	ErrValTooLarge   = errors.New("Value too large")
	ErrInvalidArgs   = errors.New("Invalid arguments")
	ErrItemNotStored = errors.New("Item not stored")
	ErrIncDecInval   = errors.New("Incr/Decr on non-numeric value.")
	ErrVBucket       = errors.New("The vbucket belongs to another server")
	ErrAuth          = errors.New("Authentication error")
	ErrAuthCont      = errors.New("Authentication continue")
	ErrUnknownCmd    = errors.New("Unknown command")
	ErrNoMem         = errors.New("Out of memory")
	ErrNotSupported  = errors.New("Not supported")
	ErrInternal      = errors.New("Internal error")
	ErrBusy          = errors.New("Busy")
	ErrTemp          = errors.New("Temporary failure")
)

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
