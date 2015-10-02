package stream

import "io"
import "math"

func ChunkLimitReader(reader io.Reader, chunkSize, totalSize int64) ChunkedLimitedReader {
    numChunks := int64(math.Ceil(float64(totalSize) / float64(chunkSize)))
    return ChunkedLimitedReader {
        d: &clrData {
            reader:     reader,
            remaining:  totalSize,
            chunkSize:  chunkSize,
            chunkRem:   chunkSize,
            numChunks:  numChunks,
            doneChunks: 0,
        },
    }
}

// will read *past* the end of the total size to fill in the remainder of a chunk
// effectively acts as a chunk iterator over the input stream
type ChunkedLimitedReader struct {
    d *clrData
}

type clrData struct {
    reader     io.Reader // underlying reader
    remaining  int64     // bytes remaining in total
    chunkSize  int64     // chunk size
    chunkRem   int64     // bytes remaining in chunk
    numChunks  int64     // total number of chunks to allow
    doneChunks int64     // number of chunks completed
}

func (c ChunkedLimitedReader) Read(p []byte) (n int, err error) {
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

func (c *ChunkedLimitedReader) NextChunk() {
    if (c.d.doneChunks < c.d.numChunks) {
        c.d.doneChunks++
        c.d.chunkRem = c.d.chunkSize
    }
}

func (c *ChunkedLimitedReader) More() bool {
    return c.d.doneChunks < c.d.numChunks
}
