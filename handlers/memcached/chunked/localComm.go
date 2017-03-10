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

package chunked

import (
	"bufio"
	"io"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/protocol/binprot"
)

// TODO: replace sending new empty metadata on miss with emptyMeta
var emptyMeta = metadata{}

func getAndTouchMetadata(rw *bufio.ReadWriter, key []byte, exptime uint32) ([]byte, metadata, error) {
	metaKey := metaKey(key)
	if err := binprot.WriteGATCmd(rw, metaKey, exptime, 0); err != nil {
		return nil, emptyMeta, err
	}
	metaData, err := getMetadataCommon(rw)
	return metaKey, metaData, err
}

func getMetadata(rw *bufio.ReadWriter, key []byte) ([]byte, metadata, error) {
	metaKey := metaKey(key)
	if err := binprot.WriteGetCmd(rw, metaKey, 0); err != nil {
		return nil, emptyMeta, err
	}
	metaData, err := getMetadataCommon(rw)
	return metaKey, metaData, err
}

func getMetadataCommon(rw *bufio.ReadWriter) (metadata, error) {
	if err := rw.Flush(); err != nil {
		return emptyMeta, err
	}

	resHeader, err := binprot.ReadResponseHeader(rw)
	if err != nil {
		return emptyMeta, err
	}
	defer binprot.PutResponseHeader(resHeader)

	err = binprot.DecodeError(resHeader)
	if err != nil {
		// read in the message "Not found" after a miss
		n, ioerr := rw.Discard(int(resHeader.TotalBodyLength))
		metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
		if ioerr != nil {
			return emptyMeta, ioerr
		}
		return emptyMeta, err
	}

	// we currently do nothing with the flags
	//buf := make([]byte, 4)
	//n, err := io.ReadAtLeast(rw, buf, 4)
	//metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
	//if err != nil {
	//	return emptyMeta, err
	//}
	//serverFlags := binary.BigEndian.Uint32(buf)

	// instead of reading and parsing flags, just discard
	rw.Discard(4)
	metrics.IncCounterBy(common.MetricBytesReadLocal, 4)

	metaData, err := readMetadata(rw)
	if err != nil {
		return emptyMeta, err
	}

	return metaData, nil
}

func simpleCmdLocal(rw *bufio.ReadWriter, flush bool) error {
	if flush {
		if err := rw.Flush(); err != nil {
			return err
		}
	}

	resHeader, err := binprot.ReadResponseHeader(rw)
	if err != nil {
		return err
	}

	n, ioerr := rw.Discard(int(resHeader.TotalBodyLength))
	metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
	if ioerr != nil {
		binprot.PutResponseHeader(resHeader)
		return ioerr
	}

	binprot.PutResponseHeader(resHeader)
	return binprot.DecodeError(resHeader)
}

func getLocalIntoBuf(rw *bufio.Reader, metaData metadata, tokenBuf, dataBuf []byte, chunkNum, totalDataLength int) (opcodeNoop bool, err error) {
	resHeader, err := binprot.ReadResponseHeader(rw)
	if err != nil {
		return false, err
	}
	defer binprot.PutResponseHeader(resHeader)

	// it feels a bit dirty knowing about batch gets here, but it's the most logical place to put
	// a check for an opcode that signals the end of a batch get or GAT. This code is a bit too big
	// to copy-paste in multiple places.
	if resHeader.Opcode == binprot.OpcodeNoop {
		return true, nil
	}

	err = binprot.DecodeError(resHeader)
	if err != nil {
		// read in the message "Not found" after a miss
		n, ioerr := rw.Discard(int(resHeader.TotalBodyLength))
		metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
		if ioerr != nil {
			return false, ioerr
		}
		return false, err
	}

	// we currently do nothing with the flags
	//buf := make([]byte, 4)
	//n, err := io.ReadAtLeast(rw, buf, 4)
	//metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
	//if err != nil {
	//	return emptyMeta, err
	//}
	//serverFlags := binary.BigEndian.Uint32(buf)

	// instead of reading and parsing flags, just discard
	rw.Discard(4)
	metrics.IncCounterBy(common.MetricBytesReadLocal, 4)

	// Read in token if requested
	if tokenBuf != nil {
		n, err := io.ReadAtLeast(rw, tokenBuf, tokenSize)
		metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
		if err != nil {
			return false, err
		}
	}

	// indices for slicing, end exclusive
	start, end := chunkSliceIndices(int(metaData.ChunkSize), chunkNum, int(metaData.Length))
	// read data directly into buf
	chunkBuf := dataBuf[start:end]

	// Read in value
	n, err := io.ReadAtLeast(rw, chunkBuf, len(chunkBuf))
	metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
	if err != nil {
		return false, err
	}

	// consume padding at end of chunk if needed
	if len(chunkBuf) < totalDataLength {
		n, ioerr := rw.Discard(totalDataLength - len(chunkBuf))
		metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
		if ioerr != nil {
			return false, ioerr
		}
	}

	return false, nil
}
