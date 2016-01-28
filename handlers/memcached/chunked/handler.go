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
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"math"

	"github.com/netflix/rend/binprot"
	"github.com/netflix/rend/common"
)

// Chunk size, leaving room for the token
// Make sure the value subtracted from chunk size stays in sync
// with the size of the Metadata struct
const chunkSize = 1024 - 16
const fullDataSize = 1024

func readResponseHeader(r *bufio.Reader) (binprot.ResponseHeader, error) {
	resHeader, err := binprot.ReadResponseHeader(r)
	if err != nil {
		return binprot.ResponseHeader{}, err
	}

	if err := binprot.DecodeError(resHeader); err != nil {
		return resHeader, err
	}

	return resHeader, nil
}

type Handler struct {
	rw   *bufio.ReadWriter
	conn io.ReadWriteCloser
}

func NewHandler(conn io.ReadWriteCloser) Handler {
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	return Handler{
		rw:   rw,
		conn: conn,
	}
}

func (h Handler) reset() {
	h.rw.Reader.Reset(bufio.NewReader(h.conn))
	h.rw.Writer.Reset(bufio.NewWriter(h.conn))
}

// Closes the Handler's underlying io.ReadWriteCloser.
// Any calls to the handler after a Close() are invalid.
func (h Handler) Close() error {
	return h.conn.Close()
}

func (h Handler) Set(cmd common.SetRequest, src *bufio.Reader) error {
	// For writing chunks, the specialized chunked reader is appropriate.
	// for unchunked, a limited reader will be needed since the text protocol
	// includes a /r/n at the end and there's no EOF to be had with a long-lived
	// connection.
	limChunkReader := newChunkLimitedReader(bytes.NewBuffer(cmd.Data), int64(chunkSize), int64(len(cmd.Data)))
	numChunks := int(math.Ceil(float64(len(cmd.Data)) / float64(chunkSize)))
	token := <-tokens

	metaKey := metaKey(cmd.Key)
	metaData := metadata{
		Length:    uint32(len(cmd.Data)),
		OrigFlags: cmd.Flags,
		NumChunks: uint32(numChunks),
		ChunkSize: chunkSize,
		Token:     token,
	}

	metaDataBuf := new(bytes.Buffer)
	binary.Write(metaDataBuf, binary.BigEndian, metaData)

	// Write metadata key
	// TODO: should there be a unique flags value for chunked data?
	if err := binprot.WriteSetCmd(h.rw.Writer, metaKey, cmd.Flags, cmd.Exptime, metadataSize); err != nil {
		return err
	}
	// Write value
	if _, err := io.Copy(h.rw.Writer, metaDataBuf); err != nil {
		return err
	}
	if err := h.rw.Flush(); err != nil {
		return err
	}

	// Read server's response
	resHeader, err := readResponseHeader(h.rw.Reader)
	if err != nil {
		// Discard request body
		if _, ioerr := src.Discard(len(cmd.Data)); ioerr != nil {
			return ioerr
		}

		// Discard response body
		if _, ioerr := h.rw.Discard(int(resHeader.TotalBodyLength)); ioerr != nil {
			return ioerr
		}

		return err
	}

	// Write all the data chunks
	// TODO: Clean up if a data chunk write fails
	// Failure can mean the write failing at the I/O level
	// or at the memcached level, e.g. response == ERROR
	chunkNum := 0
	for limChunkReader.More() {
		// Build this chunk's key
		key := chunkKey(cmd.Key, chunkNum)

		// Write the key
		if err := binprot.WriteSetCmd(h.rw.Writer, key, cmd.Flags, cmd.Exptime, fullDataSize); err != nil {
			return err
		}
		// Write token
		if _, err := h.rw.Write(token[:]); err != nil {
			return err
		}
		// Write value
		if _, err := io.Copy(h.rw.Writer, limChunkReader); err != nil {
			return err
		}
		// There's some additional overhead here calling Flush() because it causes a write() syscall
		// The set case is already a slow path and is async from the client perspective for our use
		// case so this is not a problem.
		if err := h.rw.Flush(); err != nil {
			return err
		}

		// Read server's response
		resHeader, err = readResponseHeader(h.rw.Reader)
		if err != nil {
			// Reset the ReadWriter to prevent sending garbage to memcached
			// otherwise we get disconnected
			h.reset()

			// Discard request body
			// This is more complicated code but more straightforward than attempting to get at
			// the underlying reader and discard directly, since we don't exactly know how many
			// bytes were sent already
			for limChunkReader.More() {
				if _, ioerr := io.Copy(ioutil.Discard, limChunkReader); ioerr != nil {
					return ioerr
				}

				limChunkReader.NextChunk()
			}

			// Discard repsonse body
			if _, ioerr := h.rw.Discard(int(resHeader.TotalBodyLength)); ioerr != nil {
				return ioerr
			}

			return err
		}

		// Reset for next iteration
		limChunkReader.NextChunk()
		chunkNum++
	}

	return nil
}

func (h Handler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	// No buffering here so there's not multiple gets in memory
	dataOut := make(chan common.GetResponse)
	errorOut := make(chan error)
	go realHandleGet(cmd, dataOut, errorOut, h.rw)
	return dataOut, errorOut
}

func realHandleGet(cmd common.GetRequest, dataOut chan common.GetResponse, errorOut chan error, rw *bufio.ReadWriter) {
	// read index
	// make buf
	// for numChunks do
	//   read chunk directly into buffer
	// send response

	defer close(errorOut)
	defer close(dataOut)

outer:
	for idx, key := range cmd.Keys {
		errResponse := common.GetResponse{
			Miss:   true,
			Quiet:  cmd.Quiet[idx],
			Opaque: cmd.Opaques[idx],
			Flags:  0,
			Key:    key,
			Data:   nil,
		}

		_, metaData, err := getMetadata(rw, key)
		if err != nil {
			if err == common.ErrKeyNotFound {
				//fmt.Println("Get miss because of missing metadata. Key:", key)
				dataOut <- errResponse
				continue outer
			}

			errorOut <- err
			return
		}

		errResponse.Flags = metaData.OrigFlags
		// Write all the get commands before reading
		for i := 0; i < int(metaData.NumChunks); i++ {
			chunkKey := chunkKey(key, i)
			if err := binprot.WriteGetQCmd(rw.Writer, chunkKey); err != nil {
				errorOut <- err
				return
			}
		}

		// The final command must be Get or Noop to guarantee a response
		// We use Noop to make coding easier, but it's (very) slightly less efficient
		// since we send 24 extra bytes in each direction
		if err := binprot.WriteNoopCmd(rw.Writer); err != nil {
			errorOut <- err
			return
		}

		// Flush to make sure all the get commands are sent to the server.
		if err := rw.Flush(); err != nil {
			errorOut <- err
			return
		}

		dataBuf := make([]byte, metaData.Length)
		tokenBuf := make([]byte, tokenSize)

		// Now that all the headers are sent, start reading in the data chunks. We read until the
		// header for the Noop command comes back, keeping track of how many chunks are read. This
		// means that there is no fast fail when a chunk is missing, but at least all the data is
		// read in so there's no problem with unread, buffered data that should have been discarded.
		// If the number of chunks doesn't match, we throw away the data and call it a miss.
		opcodeNoop := false
		chunk := 0
		for !opcodeNoop {
			opcodeNoop, err = getLocalIntoBuf(rw.Reader, metaData, tokenBuf, dataBuf, chunk, int(metaData.ChunkSize))
			if err != nil {
				errorOut <- err
				return
			}

			if !bytes.Equal(metaData.Token[:], tokenBuf) {
				//fmt.Println("Get miss because of invalid chunk token. Cmd:", getCmd)
				//fmt.Printf("Expected: %v\n", metaData.Token)
				//fmt.Printf("Got:      %v\n", tokenBuf)
				dataOut <- errResponse
				continue outer
			}

			// keeping track of chunks read
			chunk++
		}

		if chunk < int(metaData.NumChunks-1) {
			//fmt.Println("Get miss because of missing chunk")
			dataOut <- errResponse
			continue outer
		}

		dataOut <- common.GetResponse{
			Miss:   false,
			Quiet:  cmd.Quiet[idx],
			Opaque: cmd.Opaques[idx],
			Flags:  metaData.OrigFlags,
			Key:    key,
			Data:   dataBuf,
		}
	}
}

func (h Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	errResponse := common.GetResponse{
		Miss:   true,
		Quiet:  false,
		Opaque: cmd.Opaque,
		Flags:  0,
		Key:    cmd.Key,
		Data:   nil,
	}

	_, metaData, err := getAndTouchMetadata(h.rw, cmd.Key, cmd.Exptime)
	if err != nil {
		if err == common.ErrKeyNotFound {
			//fmt.Println("GAT miss because of missing metadata. Key:", key)
			return errResponse, nil
		}

		return common.GetResponse{}, err
	}

	errResponse.Flags = metaData.OrigFlags

	// Write all the GAT commands before reading
	for i := 0; i < int(metaData.NumChunks); i++ {
		chunkKey := chunkKey(cmd.Key, i)
		if err := binprot.WriteGATQCmd(h.rw.Writer, chunkKey, cmd.Exptime); err != nil {
			return common.GetResponse{}, err
		}
	}

	// The final command must be GAT or Noop to guarantee a response
	// We use Noop to make coding easier, but it's (very) slightly less efficient
	// since we send 24 extra bytes in each direction
	if err := binprot.WriteNoopCmd(h.rw.Writer); err != nil {
		return common.GetResponse{}, err
	}

	// Flush to make sure all the GAT commands are sent to the server.
	if err := h.rw.Flush(); err != nil {
		return common.GetResponse{}, err
	}

	dataBuf := make([]byte, metaData.Length)
	tokenBuf := make([]byte, 16)

	// Now that all the headers are sent, start reading in the data chunks. We read until the
	// header for the Noop command comes back, keeping track of how many chunks are read. This
	// means that there is no fast fail when a chunk is missing, but at least all the data is
	// read in so there's no problem with unread, buffered data that should have been discarded.
	// If the number of chunks doesn't match, we throw away the data and call it a miss.
	opcodeNoop := false
	chunk := 0
	for !opcodeNoop {
		opcodeNoop, err = getLocalIntoBuf(h.rw.Reader, metaData, tokenBuf, dataBuf, chunk, int(metaData.ChunkSize))
		if err != nil {
			if err == common.ErrKeyNotFound {
				return errResponse, nil
			}
			return common.GetResponse{}, err
		}

		if !bytes.Equal(metaData.Token[:], tokenBuf) {
			//fmt.Println("GAT miss because of invalid chunk token. Cmd:", getCmd)
			//fmt.Printf("Expected: %v\n", metaData.Token)
			//fmt.Printf("Got:      %v\n", tokenBuf)
			return errResponse, nil
		}

		// keeping track of chunks read
		chunk++
	}

	if chunk < int(metaData.NumChunks-1) {
		//fmt.Println("Get miss because of missing chunk")
		return errResponse, nil
	}

	return common.GetResponse{
		Miss:   false,
		Quiet:  false,
		Opaque: cmd.Opaque,
		Flags:  metaData.OrigFlags,
		Key:    cmd.Key,
		Data:   dataBuf,
	}, nil
}

func (h Handler) Delete(cmd common.DeleteRequest) error {
	// read metadata
	// delete metadata
	// for 0 to metadata.numChunks
	//  delete item

	metaKey, metaData, err := getMetadata(h.rw, cmd.Key)

	if err != nil {
		if err == common.ErrKeyNotFound {
			//fmt.Println("Delete miss because of missing metadata. Key:", cmd.Key)
		}
		return err
	}

	// Delete metadata first
	if err := binprot.WriteDeleteCmd(h.rw.Writer, metaKey); err != nil {
		return err
	}
	if err := simpleCmdLocal(h.rw); err != nil {
		return err
	}

	// Then delete data chunks
	for i := 0; i < int(metaData.NumChunks); i++ {
		chunkKey := chunkKey(cmd.Key, i)
		if err := binprot.WriteDeleteCmd(h.rw.Writer, chunkKey); err != nil {
			return err
		}
		if err := simpleCmdLocal(h.rw); err != nil {
			return err
		}
	}

	return nil
}

func (h Handler) Touch(cmd common.TouchRequest) error {
	// read metadata
	// for 0 to metadata.numChunks
	//  touch item
	// touch metadata

	metaKey, metaData, err := getMetadata(h.rw, cmd.Key)

	if err != nil {
		if err == common.ErrKeyNotFound {
			//fmt.Println("Touch miss because of missing metadata. Key:", cmd.Key)
			return err
		}

		return err
	}

	// First touch all the chunks
	for i := 0; i < int(metaData.NumChunks); i++ {
		chunkKey := chunkKey(cmd.Key, i)
		if err := binprot.WriteTouchCmd(h.rw.Writer, chunkKey, cmd.Exptime); err != nil {
			return err
		}
		if err := simpleCmdLocal(h.rw); err != nil {
			return err
		}
	}

	// Then touch the metadata
	if err := binprot.WriteTouchCmd(h.rw.Writer, metaKey, cmd.Exptime); err != nil {
		return err
	}
	if err := simpleCmdLocal(h.rw); err != nil {
		return err
	}

	return nil
}
