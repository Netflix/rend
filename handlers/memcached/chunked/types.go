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
	"encoding/binary"
	"io"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
)

const metadataSize = 24 + tokenSize

type metadata struct {
	Length    uint32
	OrigFlags uint32
	NumChunks uint32
	ChunkSize uint32
	Instime   uint32
	Exptime   uint32
	Token     [tokenSize]byte
}

func readMetadata(r io.Reader) (metadata, error) {
	buf := make([]byte, metadataSize)

	n, err := io.ReadAtLeast(r, buf, metadataSize)
	metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
	if err != nil {
		return emptyMeta, nil
	}

	m := metadata{}
	m.Length = binary.BigEndian.Uint32(buf[0:4])
	m.OrigFlags = binary.BigEndian.Uint32(buf[4:8])
	m.NumChunks = binary.BigEndian.Uint32(buf[8:12])
	m.ChunkSize = binary.BigEndian.Uint32(buf[12:16])
	m.Instime = binary.BigEndian.Uint32(buf[16:20])
	m.Exptime = binary.BigEndian.Uint32(buf[20:24])
	copy(m.Token[:], buf[24:])

	return m, nil
}

func writeMetadata(w io.Writer, md metadata) error {
	buf := make([]byte, metadataSize-tokenSize)

	binary.BigEndian.PutUint32(buf[0:4], md.Length)
	binary.BigEndian.PutUint32(buf[4:8], md.OrigFlags)
	binary.BigEndian.PutUint32(buf[8:12], md.NumChunks)
	binary.BigEndian.PutUint32(buf[12:16], md.ChunkSize)
	binary.BigEndian.PutUint32(buf[16:20], md.Instime)
	binary.BigEndian.PutUint32(buf[20:24], md.Exptime)

	n, err := w.Write(buf)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))
	if err != nil {
		return err
	}

	n, err = w.Write(md.Token[:])
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))
	return err
}
