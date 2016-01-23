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

import "bufio"
import "bytes"
import "encoding/binary"

import "io"
import "io/ioutil"
import "math"

import "github.com/netflix/rend/binprot"
import "github.com/netflix/rend/common"

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
	conn io.Closer
}

func NewHandler(conn io.ReadWriteCloser) Handler {
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	return Handler{
		rw:   rw,
		conn: conn,
	}
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
	limChunkReader := newChunkLimitedReader(src, int64(chunkSize), int64(cmd.Length))
	numChunks := int(math.Ceil(float64(cmd.Length) / float64(chunkSize)))
	token := <-tokens

	metaKey := metaKey(cmd.Key)
	metaData := metadata{
		Length:    cmd.Length,
		OrigFlags: cmd.Flags,
		NumChunks: uint32(numChunks),
		ChunkSize: chunkSize,
		Token:     token,
	}

	metaDataBuf := new(bytes.Buffer)
	binary.Write(metaDataBuf, binary.BigEndian, metaData)

	// Write metadata key
	// TODO: should there be a unique flags value for chunked data?
	if err := binprot.WriteSetCmd(w.rw.Writer, metaKey, cmd.Flags, cmd.Exptime, metadataSize); err != nil {
		return err
	}
	// Write value
	if _, err := io.Copy(h.rw.Writer, data); err != nil {
		return err
	}
	if err := h.rw.Writer.Flush(); err != nil {
		return err
	}

	// Read server's response
	resHeader, err := readResponseHeader(h.rw.Reader)
	if err != nil {
		// Discard request body
		if _, ioerr := src.Discard(int(cmd.Length)); ioerr != nil {
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
		// Write value
		if _, err := io.Copy(h.rw.Writer, limChunkReader); err != nil {
			return err
		}
		// There's some additional overhead here calling Flush() because it causes a write() syscall
		// The set case is already a slow path, and is async from the client perspective for our use
		// case so this is not a problem.
		if err := h.rw.Writer.Flush(); err != nil {
			return err
		}

		// Read server's response
		resHeader, err = readResponseHeader(h.rw.Reader)
		if err != nil {
			// Discard request body
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

		dataBuf := make([]byte, metaData.Length)
		tokenBuf := make([]byte, tokenSize)

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

		// Now that all the headers are sent, start reading in the data chunks. We read until the
		// header for the Noop command comes back, keeping track of how many chunks are read. This
		// means that there is no fast fail when a chunk is missing, but at least all the data is
		// read in so there's no problem with unread, buffered data that should have been discarded.
		// If the number of chunks doesn't match, we throw away the data and call it a miss.
		opcodeNoop := false
		chunk := 0
		for !opcodeNoop {
			// indices for slicing, end exclusive
			start, end := chunkSliceIndices(int(metaData.ChunkSize), i, int(metaData.Length))
			// read data directly into buf
			chunkBuf := dataBuf[start:end]

			opcodeNoop, err := getLocalIntoBuf(rw, tokenBuf, chunkBuf, int(metaData.ChunkSize))
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

		if chunk < metaData.NumChunks-1 {
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

	dataBuf := make([]byte, metaData.Length)
	tokenBuf := make([]byte, 16)

	// Write all the get commands before reading
	for i := 0; i < int(metaData.NumChunks); i++ {
		chunkKey := chunkKey(key, i)
		if err := binprot.WriteGATQCmd(h.rw.Writer, chunkKey); err != nil {
			errorOut <- err
			return
		}
	}

	// The final command must be GAT or Noop to guarantee a response
	// We use Noop to make coding easier, but it's (very) slightly less efficient
	// since we send 24 extra bytes in each direction
	if err := binprot.WriteNoopCmd(h.rw.Writer); err != nil {
		errorOut <- err
		return
	}

	// Flush to make sure all the GAT commands are sent to the server.
	if err := rw.Flush(); err != nil {
		errorOut <- err
		return
	}

	// Now that all the headers are sent, start reading in the data chunks. We read until the
	// header for the Noop command comes back, keeping track of how many chunks are read. This
	// means that there is no fast fail when a chunk is missing, but at least all the data is
	// read in so there's no problem with unread, buffered data that should have been discarded.
	// If the number of chunks doesn't match, we throw away the data and call it a miss.
	opcode := binprot.OpcodeInvalid
	chunk := 0
	for opcode != binprot.OpcodeNoop {
		// indices for slicing, end exclusive
		start, end := chunkSliceIndices(int(metaData.ChunkSize), i, int(metaData.Length))
		// read data directly into buf
		chunkBuf := dataBuf[start:end]

		if err := getLocalIntoBuf(rw, tokenBuf, chunkBuf, int(metaData.ChunkSize)); err != nil {
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

	if chunk < metaData.NumChunks-1 {
		//fmt.Println("Get miss because of missing chunk")
		dataOut <- errResponse
		continue outer
	}

	//

	//

	// gather all commands into one big buffer
	for i := 0; i < int(metaData.NumChunks); i++ {
		chunkKey := chunkKey(key, i)
		getCmd = append(getCmd, binprot.GetQCmd(chunkKey)...)
	}

	chunkKey := chunkKey(key, int(metaData.NumChunks-1))
	getCmd = append(getCmd, binprot.GetCmd(chunkKey)...)

	if _, err := rw.Write(getCmd); err != nil {
		errorOut <- err
		return
	}

	if err := rw.Flush(); err != nil {
		errorOut <- err
		return
	}

	for i := 0; i < int(metaData.NumChunks); i++ {
		// indices for slicing, end exclusive
		start, end := chunkSliceIndices(int(metaData.ChunkSize), i, int(metaData.Length))
		// read data directly into buf
		chunkBuf := dataBuf[start:end]

		if err := getLocalIntoBuf(h.rw, tokenBuf, chunkBuf, int(metaData.ChunkSize)); err != nil {
			if err == common.ErrKeyNotFound {
				//fmt.Println("GAT miss because of missing chunk. Cmd:", getCmd)
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
	}

	return common.GetResponse{
		Miss:   false,
		Quiet:  false,
		Opaque: cmd.Opaque,
		Flags:  metaData.OrigFlags,
		Key:    cmd.Key,
		Data:   nil,
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

	deleteCmd := binprot.DeleteCmd(metaKey)
	if err := simpleCmdLocal(h.rw, deleteCmd); err != nil {
		return err
	}

	for i := 0; i < int(metaData.NumChunks); i++ {
		chunkKey := chunkKey(cmd.Key, i)
		deleteCmd = binprot.DeleteCmd(chunkKey)

		if err := simpleCmdLocal(h.rw, deleteCmd); err != nil {
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

	for i := 0; i < int(metaData.NumChunks); i++ {
		chunkKey := chunkKey(cmd.Key, i)
		touchCmd := binprot.TouchCmd(chunkKey, cmd.Exptime)
		if err := simpleCmdLocal(h.rw, touchCmd); err != nil {
			return err
		}
	}

	touchCmd := binprot.TouchCmd(metaKey, cmd.Exptime)
	if err := simpleCmdLocal(h.rw, touchCmd); err != nil {
		return err
	}

	return nil
}
