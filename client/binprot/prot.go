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
	"fmt"
	"io"

	"github.com/netflix/rend/client/common"
)

type BinProt struct{}

type ErrOpaqueMismatch struct {
	expected uint32
	got      uint32
}

func (e ErrOpaqueMismatch) Error() string {
	return fmt.Sprintf("Opaques do not match: expected %d got %d", e.expected, e.got)
}

func consumeResponseE(r *bufio.Reader) ([]byte, uint32, uint32, error) {
	res, err := readRes(r)
	if err != nil {
		return nil, 0, 0, err
	}

	apperr := statusToError(res.Status)

	// read body in regardless of the error in the header
	buf := make([]byte, res.BodyLen)
	io.ReadFull(r, buf)

	var flags uint32
	var expiration uint32
	if res.ExtraLen >= 4 {
		flags = binary.BigEndian.Uint32(buf[0:4])
	}
	if res.ExtraLen >= 8 {
		expiration = binary.BigEndian.Uint32(buf[4:8])
	}
	buf = buf[res.ExtraLen:]

	resPool.Put(res)

	return buf, flags, expiration, apperr
}

func consumeResponse(r *bufio.Reader) ([]byte, error) {
	res, err := readRes(r)
	if err != nil {
		return nil, err
	}

	apperr := statusToError(res.Status)

	// read body in regardless of the error in the header
	buf := make([]byte, res.BodyLen)
	io.ReadFull(r, buf)

	// ignore extras for now
	buf = buf[res.ExtraLen:]

	resPool.Put(res)

	return buf, apperr
}

func consumeResponseCheckOpaque(r *bufio.Reader, opq int) ([]byte, error) {
	opaque := uint32(opq)
	res, err := readRes(r)
	if err != nil {
		return nil, err
	}

	apperr := statusToError(res.Status)

	// read body in regardless of the error in the header
	buf := make([]byte, res.BodyLen)
	io.ReadFull(r, buf)

	// ignore extras for now
	buf = buf[res.ExtraLen:]

	resPool.Put(res)

	if res.Opaque != opaque {
		return buf, ErrOpaqueMismatch{
			expected: opaque,
			got:      res.Opaque,
		}
	}

	return buf, apperr
}

func consumeBatchResponse(r *bufio.Reader) ([][]byte, error) {
	opcode := uint8(Get)
	var apperr error
	var ret [][]byte

	for opcode != Noop {
		res, err := readRes(r)
		if err != nil {
			return nil, err
		}

		opcode = res.Opcode
		apperr = statusToError(res.Status)

		// read body in regardless of the error in the header
		buf := make([]byte, res.BodyLen)
		io.ReadFull(r, buf)

		// ignore extras for now
		buf = buf[res.ExtraLen:]

		ret = append(ret, buf)
	}

	return ret, apperr
}

func (b BinProt) Set(rw *bufio.ReadWriter, key, value []byte) error {
	// set packet contains the req header, flags, and expiration
	// flags are irrelevant, and are thus zero.
	// expiration could be important, so hammer with random values from 1 sec up to 1 hour

	return b.SetE(rw, key, value, common.Exp())
}

func (b BinProt) SetE(rw *bufio.ReadWriter, key, value []byte, expiration uint32) error {
	// set packet contains the req header, flags, and expiration
	// flags are irrelevant, and are thus zero.
	// expiration could be important, so hammer with random values from 1 sec up to 1 hour

	// Header
	bodylen := 8 + len(key) + len(value)
	writeReq(rw, Set, len(key), 8, bodylen, 0)
	// Extras
	binary.Write(rw, binary.BigEndian, uint32(0))
	binary.Write(rw, binary.BigEndian, expiration)
	// Body / data
	rw.Write(key)
	rw.Write(value)

	rw.Flush()

	// consume all of the response and discard
	_, err := consumeResponse(rw.Reader)
	return err
}

func (b BinProt) Add(rw *bufio.ReadWriter, key, value []byte) error {
	// add packet contains the req header, flags, and expiration
	// flags are irrelevant, and are thus zero.
	// expiration could be important, so hammer with random values from 1 sec up to 1 hour

	// Header
	bodylen := 8 + len(key) + len(value)
	writeReq(rw, Add, len(key), 8, bodylen, 0)
	// Extras
	binary.Write(rw, binary.BigEndian, uint32(0))
	binary.Write(rw, binary.BigEndian, common.Exp())
	// Body / data
	rw.Write(key)
	rw.Write(value)

	rw.Flush()

	// consume all of the response and discard
	_, err := consumeResponse(rw.Reader)
	return err
}

func (b BinProt) Replace(rw *bufio.ReadWriter, key, value []byte) error {
	// replace packet contains the req header, flags, and expiration
	// flags are irrelevant, and are thus zero.
	// expiration could be important, so hammer with random values from 1 sec up to 1 hour

	// Header
	bodylen := 8 + len(key) + len(value)
	writeReq(rw, Replace, len(key), 8, bodylen, 0)
	// Extras
	binary.Write(rw, binary.BigEndian, uint32(0))
	binary.Write(rw, binary.BigEndian, common.Exp())
	// Body / data
	rw.Write(key)
	rw.Write(value)

	rw.Flush()

	// consume all of the response and discard
	_, err := consumeResponse(rw.Reader)
	return err
}

func (b BinProt) Append(rw *bufio.ReadWriter, key, value []byte) error {
	// Append packet contains the req header and key only

	// Header
	bodylen := len(key) + len(value)
	writeReq(rw, Append, len(key), 0, bodylen, 0)
	// Body / data
	rw.Write(key)
	rw.Write(value)

	rw.Flush()

	// consume all of the response and discard
	_, err := consumeResponse(rw.Reader)
	return err
}

func (b BinProt) Prepend(rw *bufio.ReadWriter, key, value []byte) error {
	// Append packet contains the req header and key only

	// Header
	bodylen := len(key) + len(value)
	writeReq(rw, Prepend, len(key), 0, bodylen, 0)
	// Body / data
	rw.Write(key)
	rw.Write(value)

	rw.Flush()

	// consume all of the response and discard
	_, err := consumeResponse(rw.Reader)
	return err
}

func (b BinProt) Get(rw *bufio.ReadWriter, key []byte) ([]byte, error) {
	// Header
	writeReq(rw, Get, len(key), 0, len(key), 0)
	// Body
	rw.Write(key)

	rw.Flush()

	// consume all of the response and return
	return consumeResponse(rw.Reader)
}

func (b BinProt) GetE(rw *bufio.ReadWriter, key []byte) ([]byte, uint32, uint32, error) {
	// Header
	writeReq(rw, GetE, len(key), 0, len(key), 0)
	// Body
	rw.Write(key)

	rw.Flush()

	// consume all of the response and return
	return consumeResponseE(rw.Reader)
}

func (b BinProt) GetWithOpaque(rw *bufio.ReadWriter, key []byte, opaque int) ([]byte, error) {
	// Header
	writeReq(rw, Get, len(key), 0, len(key), opaque)
	// Body
	rw.Write(key)

	rw.Flush()

	// consume all of the response and return
	return consumeResponseCheckOpaque(rw.Reader, opaque)
}

func (b BinProt) BatchGet(rw *bufio.ReadWriter, keys [][]byte) ([][]byte, error) {
	for _, key := range keys {
		// Header
		writeReq(rw, GetQ, len(key), 0, len(key), 0)
		// Body
		rw.Write(key)
	}

	writeReq(rw, Noop, 0, 0, 0, 0)

	rw.Flush()

	// consume all of the responses and return
	return consumeBatchResponse(rw.Reader)
}

func (b BinProt) GAT(rw *bufio.ReadWriter, key []byte) ([]byte, error) {
	// Header
	writeReq(rw, GAT, len(key), 4, len(key), 0)
	// Extras
	binary.Write(rw, binary.BigEndian, common.Exp())
	// Body
	rw.Write(key)

	rw.Flush()

	// consume all of the response and return
	return consumeResponse(rw.Reader)
}

func (b BinProt) Delete(rw *bufio.ReadWriter, key []byte) error {
	// Header
	writeReq(rw, Delete, len(key), 0, len(key), 0)
	// Body
	rw.Write(key)

	rw.Flush()

	// consume all of the response and discard
	_, err := consumeResponse(rw.Reader)
	return err
}

func (b BinProt) Touch(rw *bufio.ReadWriter, key []byte) error {
	// Header
	writeReq(rw, Touch, len(key), 4, len(key)+4, 0)
	// Extras
	binary.Write(rw, binary.BigEndian, common.Exp())
	// Body
	rw.Write(key)

	rw.Flush()

	// consume all of the response and discard
	_, err := consumeResponse(rw.Reader)
	return err
}
