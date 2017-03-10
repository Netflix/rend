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

// Data commands are those that send a header, key, exptime, and data
func writeDataCmdCommon(w io.Writer, opcode uint8, key []byte, flags, exptime, dataSize, opaque uint32) error {
	// opcode, keyLength, extraLength, totalBodyLength
	// key + extras + body
	extrasLen := 8
	totalBodyLength := len(key) + extrasLen + int(dataSize)
	header := makeRequestHeader(opcode, len(key), extrasLen, totalBodyLength, opaque)

	writeRequestHeader(w, header)

	buf := make([]byte, len(key)+8)
	binary.BigEndian.PutUint32(buf[0:4], flags)
	binary.BigEndian.PutUint32(buf[4:8], exptime)
	copy(buf[8:], key)

	n, err := w.Write(buf)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))

	reqHeadPool.Put(header)

	return err
}

// WriteSetCmd writes out the binary representation of a set request header to the given io.Writer
func WriteSetCmd(w io.Writer, key []byte, flags, exptime, dataSize, opaque uint32) error {
	//fmt.Printf("Set: key: %v | flags: %v | exptime: %v | dataSize: %v | totalBodyLength: %v\n",
	//string(key), flags, exptime, dataSize, totalBodyLength)
	return writeDataCmdCommon(w, OpcodeSet, key, flags, exptime, dataSize, opaque)
}

// WriteAddCmd writes out the binary representation of an add request header to the given io.Writer
func WriteAddCmd(w io.Writer, key []byte, flags, exptime, dataSize, opaque uint32) error {
	//fmt.Printf("Add: key: %v | flags: %v | exptime: %v | dataSize: %v | totalBodyLength: %v\n",
	//string(key), flags, exptime, dataSize, totalBodyLength)
	return writeDataCmdCommon(w, OpcodeAdd, key, flags, exptime, dataSize, opaque)
}

// WriteReplaceCmd writes out the binary representation of a replace request header to the given io.Writer
func WriteReplaceCmd(w io.Writer, key []byte, flags, exptime, dataSize, opaque uint32) error {
	//fmt.Printf("Replace: key: %v | flags: %v | exptime: %v | dataSize: %v | totalBodyLength: %v\n",
	//string(key), flags, exptime, dataSize, totalBodyLength)
	return writeDataCmdCommon(w, OpcodeReplace, key, flags, exptime, dataSize, opaque)
}

func writeAppendPrependCmdCommon(w io.Writer, opcode uint8, key []byte, flags, exptime, dataSize, opaque uint32) error {
	// opcode, keyLength, extraLength, totalBodyLength
	// key + body
	totalBodyLength := len(key) + int(dataSize)
	header := makeRequestHeader(opcode, len(key), 0, totalBodyLength, opaque)

	writeRequestHeader(w, header)

	n, err := w.Write(key)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))

	reqHeadPool.Put(header)

	return err
}

// WriteAppendCmd writes out the binary representation of an append request header to the given io.Writer
func WriteAppendCmd(w io.Writer, key []byte, flags, exptime, dataSize, opaque uint32) error {
	//fmt.Printf("Append: key: %v | flags: %v | exptime: %v | dataSize: %v | totalBodyLength: %v\n",
	//string(key), flags, exptime, dataSize, totalBodyLength)
	return writeAppendPrependCmdCommon(w, OpcodeAppend, key, flags, exptime, dataSize, opaque)
}

// WritePrependCmd writes out the binary representation of a prepend request header to the given io.Writer
func WritePrependCmd(w io.Writer, key []byte, flags, exptime, dataSize, opaque uint32) error {
	//fmt.Printf("Prepend: key: %v | flags: %v | exptime: %v | dataSize: %v | totalBodyLength: %v\n",
	//string(key), flags, exptime, dataSize, totalBodyLength)
	return writeAppendPrependCmdCommon(w, OpcodePrepend, key, flags, exptime, dataSize, opaque)
}

// Key commands send the header and key only
func writeKeyCmd(w io.Writer, opcode uint8, key []byte, opaque uint32) error {
	// opcode, keyLength, extraLength, totalBodyLength
	header := makeRequestHeader(opcode, len(key), 0, len(key), opaque)
	writeRequestHeader(w, header)

	n, err := w.Write(key)

	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(ReqHeaderLen+n))
	reqHeadPool.Put(header)

	return err
}

// WriteGetCmd writes out the binary representation of a get request header to the given io.Writer
func WriteGetCmd(w io.Writer, key []byte, opaque uint32) error {
	//fmt.Printf("Get: key: %v | totalBodyLength: %v\n", string(key), len(key))
	return writeKeyCmd(w, OpcodeGet, key, opaque)
}

// WriteGetQCmd writes out the binary representation of a getq request header to the given io.Writer
func WriteGetQCmd(w io.Writer, key []byte, opaque uint32) error {
	//fmt.Printf("GetQ: key: %v | totalBodyLength: %v\n", string(key), len(key))
	return writeKeyCmd(w, OpcodeGetQ, key, opaque)
}

// WriteGetECmd writes out the binary representation of a gete request header to the given io.Writer
func WriteGetECmd(w io.Writer, key []byte, opaque uint32) error {
	//fmt.Printf("GetE: key: %v | totalBodyLength: %v\n", string(key), len(key))
	return writeKeyCmd(w, OpcodeGetE, key, opaque)
}

// WriteGetEQCmd writes out the binary representation of a geteq request header to the given io.Writer
func WriteGetEQCmd(w io.Writer, key []byte, opaque uint32) error {
	//fmt.Printf("GetEQ: key: %v | totalBodyLength: %v\n", string(key), len(key))
	return writeKeyCmd(w, OpcodeGetEQ, key, opaque)
}

// WriteDeleteCmd writes out the binary representation of a delete request header to the given io.Writer
func WriteDeleteCmd(w io.Writer, key []byte, opaque uint32) error {
	//fmt.Printf("Delete: key: %v | totalBodyLength: %v\n", string(key), len(key))
	return writeKeyCmd(w, OpcodeDelete, key, opaque)
}

// Key Exptime commands send the header, key, and an exptime
func writeKeyExptimeCmd(w io.Writer, opcode uint8, key []byte, exptime, opaque uint32) error {
	// opcode, keyLength, extraLength, totalBodyLength
	// key + extras + body
	extrasLen := 4
	totalBodyLength := len(key) + extrasLen
	header := makeRequestHeader(opcode, len(key), extrasLen, totalBodyLength, opaque)

	writeRequestHeader(w, header)

	buf := make([]byte, len(key)+4)
	binary.BigEndian.PutUint32(buf[0:4], exptime)
	copy(buf[4:], key)

	n, err := w.Write(buf)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))

	reqHeadPool.Put(header)

	return err
}

// WriteTouchCmd writes out the binary representation of a touch request header to the given io.Writer
func WriteTouchCmd(w io.Writer, key []byte, exptime, opaque uint32) error {
	//fmt.Printf("Touch: key: %v | exptime: %v | totalBodyLength: %v\n", string(key),
	//exptime, totalBodyLength)
	return writeKeyExptimeCmd(w, OpcodeTouch, key, exptime, opaque)
}

// WriteGATCmd writes out the binary representation of a get-and-touch request header to the given io.Writer
func WriteGATCmd(w io.Writer, key []byte, exptime, opaque uint32) error {
	//fmt.Printf("GAT: key: %v | exptime: %v | totalBodyLength: %v\n", string(key),
	//exptime, len(key))
	return writeKeyExptimeCmd(w, OpcodeGat, key, exptime, opaque)
}

// WriteGATQCmd writes out the binary representation of a get-and-touch quiet request header to the given io.Writer
func WriteGATQCmd(w io.Writer, key []byte, exptime, opaque uint32) error {
	//fmt.Printf("GATQ: key: %v | exptime: %v | totalBodyLength: %v\n", string(key),
	//exptime, len(key))
	return writeKeyExptimeCmd(w, OpcodeGatQ, key, exptime, opaque)
}

// WriteNoopCmd writes out the binary representation of a noop request header to the given io.Writer
func WriteNoopCmd(w io.Writer, opaque uint32) error {
	// opcode, keyLength, extraLength, totalBodyLength
	header := makeRequestHeader(OpcodeNoop, 0, 0, 0, opaque)
	//fmt.Printf("Delete: key: %v | totalBodyLength: %v\n", string(key), len(key))

	err := writeRequestHeader(w, header)

	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(ReqHeaderLen))

	reqHeadPool.Put(header)

	return err
}
