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
	"encoding/binary"
	"io"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
)

func WriteSetCmd(w io.Writer, key []byte, flags, exptime, dataSize uint32) error {
	// opcode, keyLength, extraLength, totalBodyLength
	// key + extras + body
	extrasLen := 8
	totalBodyLength := len(key) + extrasLen + int(dataSize)
	header := MakeRequestHeader(OpcodeSet, len(key), extrasLen, totalBodyLength)

	//fmt.Printf("Set: key: %v | flags: %v | exptime: %v | dataSize: %v | totalBodyLength: %v\n",
	//string(key), flags, exptime, dataSize, totalBodyLength)

	binary.Write(w, binary.BigEndian, header)
	binary.Write(w, binary.BigEndian, flags)
	binary.Write(w, binary.BigEndian, exptime)
	err := binary.Write(w, binary.BigEndian, key)

	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(ReqHeaderLen+totalBodyLength))

	return err
}

func WriteAddCmd(w io.Writer, key []byte, flags, exptime, dataSize uint32) error {
	// opcode, keyLength, extraLength, totalBodyLength
	// key + extras + body
	extrasLen := 8
	totalBodyLength := len(key) + extrasLen + int(dataSize)
	header := MakeRequestHeader(OpcodeAdd, len(key), extrasLen, totalBodyLength)

	//fmt.Printf("Add: key: %v | flags: %v | exptime: %v | dataSize: %v | totalBodyLength: %v\n",
	//string(key), flags, exptime, dataSize, totalBodyLength)

	binary.Write(w, binary.BigEndian, header)
	binary.Write(w, binary.BigEndian, flags)
	binary.Write(w, binary.BigEndian, exptime)
	err := binary.Write(w, binary.BigEndian, key)

	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(ReqHeaderLen+totalBodyLength))

	return err
}

func WriteGetCmd(w io.Writer, key []byte) error {
	// opcode, keyLength, extraLength, totalBodyLength
	header := MakeRequestHeader(OpcodeGet, len(key), 0, len(key))

	//fmt.Printf("Get: key: %v | totalBodyLength: %v\n", string(key), len(key))

	binary.Write(w, binary.BigEndian, header)
	err := binary.Write(w, binary.BigEndian, key)

	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(ReqHeaderLen+len(key)))

	return err
}

func WriteGetQCmd(w io.Writer, key []byte) error {
	// opcode, keyLength, extraLength, totalBodyLength
	header := MakeRequestHeader(OpcodeGetQ, len(key), 0, len(key))

	//fmt.Printf("GetQ: key: %v | totalBodyLength: %v\n", string(key), len(key))

	binary.Write(w, binary.BigEndian, header)
	err := binary.Write(w, binary.BigEndian, key)

	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(ReqHeaderLen+len(key)))

	return err
}

func WriteGATCmd(w io.Writer, key []byte, exptime uint32) error {
	// opcode, keyLength, extraLength, totalBodyLength
	extrasLen := 4
	totalBodyLength := len(key) + extrasLen
	header := MakeRequestHeader(OpcodeGat, len(key), extrasLen, totalBodyLength)

	//fmt.Printf("GAT: key: %v | exptime: %v | totalBodyLength: %v\n", string(key),
	//exptime, len(key))

	binary.Write(w, binary.BigEndian, header)
	binary.Write(w, binary.BigEndian, exptime)
	err := binary.Write(w, binary.BigEndian, key)

	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(ReqHeaderLen+totalBodyLength))

	return err
}

func WriteGATQCmd(w io.Writer, key []byte, exptime uint32) error {
	// opcode, keyLength, extraLength, totalBodyLength
	extrasLen := 4
	totalBodyLength := len(key) + extrasLen
	header := MakeRequestHeader(OpcodeGatQ, len(key), extrasLen, totalBodyLength)

	//fmt.Printf("GAT: key: %v | exptime: %v | totalBodyLength: %v\n", string(key),
	//exptime, len(key))

	binary.Write(w, binary.BigEndian, header)
	binary.Write(w, binary.BigEndian, exptime)
	err := binary.Write(w, binary.BigEndian, key)

	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(ReqHeaderLen+totalBodyLength))

	return err
}

func WriteDeleteCmd(w io.Writer, key []byte) error {
	// opcode, keyLength, extraLength, totalBodyLength
	header := MakeRequestHeader(OpcodeDelete, len(key), 0, len(key))

	//fmt.Printf("Delete: key: %v | totalBodyLength: %v\n", string(key), len(key))

	binary.Write(w, binary.BigEndian, header)
	err := binary.Write(w, binary.BigEndian, key)

	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(ReqHeaderLen+len(key)))

	return err
}

func WriteTouchCmd(w io.Writer, key []byte, exptime uint32) error {
	// opcode, keyLength, extraLength, totalBodyLength
	// key + extras + body
	extrasLen := 4
	totalBodyLength := len(key) + extrasLen
	header := MakeRequestHeader(OpcodeTouch, len(key), extrasLen, totalBodyLength)

	//fmt.Printf("GAT: key: %v | exptime: %v | totalBodyLength: %v\n", string(key),
	//exptime, totalBodyLength)

	binary.Write(w, binary.BigEndian, header)
	binary.Write(w, binary.BigEndian, exptime)
	err := binary.Write(w, binary.BigEndian, key)

	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(ReqHeaderLen+totalBodyLength))

	return err
}

func WriteNoopCmd(w io.Writer) error {
	// opcode, keyLength, extraLength, totalBodyLength
	header := MakeRequestHeader(OpcodeNoop, 0, 0, 0)
	//fmt.Printf("Delete: key: %v | totalBodyLength: %v\n", string(key), len(key))

	err := binary.Write(w, binary.BigEndian, header)

	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(ReqHeaderLen))

	return err
}
