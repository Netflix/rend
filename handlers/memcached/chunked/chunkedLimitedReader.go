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

import "io"
import "math"

func newChunkLimitedReader(reader io.Reader, chunkSize, totalSize int64) chunkedLimitedReader {
	numChunks := int64(math.Ceil(float64(totalSize) / float64(chunkSize)))
	return chunkedLimitedReader{
		d: &clrData{
			reader:     reader,
			remaining:  totalSize,
			chunkSize:  chunkSize,
			chunkRem:   chunkSize,
			numChunks:  numChunks,
			doneChunks: 0,
		},
	}
}

// This reader is ***NOT THREAD SAFE***
// It will read *past* the end of the total size to fill in the remainder of a chunk
// effectively acts as a chunk iterator over the input stream
type chunkedLimitedReader struct {
	d *clrData
}

// Because the Read method is value-only, we need to keep our state in another struct
// and maintain a pointer to that struct in the main reader struct.
type clrData struct {
	reader     io.Reader // underlying reader
	remaining  int64     // bytes remaining in total
	chunkSize  int64     // chunk size
	chunkRem   int64     // bytes remaining in chunk
	numChunks  int64     // total number of chunks to allow
	doneChunks int64     // number of chunks completed
}

// io.Reader's interface implements this as a value method, not a pointer method.
func (c chunkedLimitedReader) Read(p []byte) (n int, err error) {
	// If we've already read all our chunks and the remainders are <= 0, we're done
	if c.d.doneChunks >= c.d.numChunks || (c.d.remaining <= 0 && c.d.chunkRem <= 0) {
		return 0, io.EOF
	}

	// Data is done, returning only buffer bytes now
	if c.d.remaining <= 0 {
		if int64(len(p)) > c.d.chunkRem {
			p = p[0:c.d.chunkRem]
		}

		for i := range p {
			p[i] = 0
		}

		c.d.chunkRem -= int64(len(p))
		return len(p), nil
	}

	// Data is not yet done, but chunk is
	if c.d.chunkRem <= 0 {
		return 0, io.EOF
	}

	// Data and chunk not yet done, need to read from outside reader
	if int64(len(p)) > c.d.remaining || int64(len(p)) > c.d.chunkRem {
		rem := int64(math.Min(float64(c.d.remaining), float64(c.d.chunkRem)))
		p = p[0:rem]
	}

	n, err = c.d.reader.Read(p)
	c.d.remaining -= int64(n)
	c.d.chunkRem -= int64(n)

	return
}

func (c chunkedLimitedReader) NextChunk() {
	if c.d.doneChunks < c.d.numChunks {
		c.d.doneChunks++
		c.d.chunkRem = c.d.chunkSize
	}
}

func (c chunkedLimitedReader) More() bool {
	return c.d.doneChunks < c.d.numChunks
}
