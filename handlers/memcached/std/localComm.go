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

package std

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/protocol/binprot"
)

func simpleCmdLocal(rw *bufio.ReadWriter) error {
	if err := rw.Flush(); err != nil {
		return err
	}

	resHeader, err := binprot.ReadResponseHeader(rw)
	if err != nil {
		return err
	}
	defer binprot.PutResponseHeader(resHeader)

	err = binprot.DecodeError(resHeader)
	if err != nil {
		n, ioerr := rw.Discard(int(resHeader.TotalBodyLength))
		metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
		if ioerr != nil {
			return ioerr
		}
		return err
	}

	// Read in the message bytes from the body
	n, err := rw.Discard(int(resHeader.TotalBodyLength))
	metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))

	return err
}

func getLocal(rw *bufio.ReadWriter, readExp bool) (data []byte, flags, exp uint32, err error) {
	if err := rw.Flush(); err != nil {
		return nil, 0, 0, err
	}

	resHeader, err := binprot.ReadResponseHeader(rw)
	if err != nil {
		return nil, 0, 0, err
	}
	defer binprot.PutResponseHeader(resHeader)

	err = binprot.DecodeError(resHeader)
	if err != nil {
		n, ioerr := rw.Discard(int(resHeader.TotalBodyLength))
		metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
		if ioerr != nil {
			return nil, 0, 0, ioerr
		}
		return nil, 0, 0, err
	}

	var serverFlags uint32
	binary.Read(rw, binary.BigEndian, &serverFlags)
	metrics.IncCounterBy(common.MetricBytesReadLocal, 4)

	var serverExp uint32
	if readExp {
		binary.Read(rw, binary.BigEndian, &serverExp)
		metrics.IncCounterBy(common.MetricBytesReadLocal, 4)
	}

	// total body - key - extra
	dataLen := resHeader.TotalBodyLength - uint32(resHeader.KeyLength) - uint32(resHeader.ExtraLength)
	buf := make([]byte, dataLen)

	// Read in value
	n, err := io.ReadAtLeast(rw, buf, int(dataLen))
	metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
	if err != nil {
		return nil, 0, 0, err
	}

	return buf, serverFlags, serverExp, nil
}
