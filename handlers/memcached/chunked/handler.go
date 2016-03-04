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
	"math"

	"github.com/netflix/rend/binprot"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
)

const (
	// Chunk size, leaving room for the token
	// Make sure the value subtracted from chunk size stays in sync
	// with the size of the Metadata struct
	chunkSize    = 1024 - 16
	fullDataSize = 1024
)

var (
	MetricCmdSetErrorsOOM        = metrics.AddCounter("cmd_set_errors_oom")
	MetricCmdSetErrorsOOML1      = metrics.AddCounter("cmd_set_errors_oom_l1")
	MetricCmdSetErrorsOOML2      = metrics.AddCounter("cmd_set_errors_oom_l2")
	MetricCmdTouchMissesMeta     = metrics.AddCounter("cmd_touch_misses_meta")
	MetricCmdTouchMissesMetaL1   = metrics.AddCounter("cmd_touch_misses_meta_l1")
	MetricCmdTouchMissesMetaL2   = metrics.AddCounter("cmd_touch_misses_meta_l2")
	MetricCmdTouchMissesChunk    = metrics.AddCounter("cmd_touch_misses_chunk")
	MetricCmdTouchMissesChunkL1  = metrics.AddCounter("cmd_touch_misses_chunk_l1")
	MetricCmdTouchMissesChunkL2  = metrics.AddCounter("cmd_touch_misses_chunk_l2")
	MetricCmdDeleteMissesMeta    = metrics.AddCounter("cmd_delete_misses_meta")
	MetricCmdDeleteMissesMetaL1  = metrics.AddCounter("cmd_delete_misses_meta_l1")
	MetricCmdDeleteMissesMetaL2  = metrics.AddCounter("cmd_delete_misses_meta_l2")
	MetricCmdDeleteMissesChunk   = metrics.AddCounter("cmd_delete_misses_chunk")
	MetricCmdDeleteMissesChunkL1 = metrics.AddCounter("cmd_delete_misses_chunk_l1")
	MetricCmdDeleteMissesChunkL2 = metrics.AddCounter("cmd_delete_misses_chunk_l2")
	MetricCmdGetMissesMeta       = metrics.AddCounter("cmd_get_misses_meta")
	MetricCmdGetMissesMetaL1     = metrics.AddCounter("cmd_get_misses_meta_l1")
	MetricCmdGetMissesMetaL2     = metrics.AddCounter("cmd_get_misses_meta_l2")
	MetricCmdGetMissesChunk      = metrics.AddCounter("cmd_get_misses_chunk")
	MetricCmdGetMissesChunkL1    = metrics.AddCounter("cmd_get_misses_chunk_l1")
	MetricCmdGetMissesChunkL2    = metrics.AddCounter("cmd_get_misses_chunk_l2")
	MetricCmdGetMissesToken      = metrics.AddCounter("cmd_get_misses_token")
	MetricCmdGetMissesTokenL1    = metrics.AddCounter("cmd_get_misses_token_l1")
	MetricCmdGetMissesTokenL2    = metrics.AddCounter("cmd_get_misses_token_l2")
	MetricCmdGatMissesMeta       = metrics.AddCounter("cmd_gat_misses_meta")
	MetricCmdGatMissesMetaL1     = metrics.AddCounter("cmd_gat_misses_meta_l1")
	MetricCmdGatMissesMetaL2     = metrics.AddCounter("cmd_gat_misses_meta_l2")
	MetricCmdGatMissesChunk      = metrics.AddCounter("cmd_gat_misses_chunk")
	MetricCmdGatMissesChunkL1    = metrics.AddCounter("cmd_gat_misses_chunk_l1")
	MetricCmdGatMissesChunkL2    = metrics.AddCounter("cmd_gat_misses_chunk_l2")
	MetricCmdGatMissesToken      = metrics.AddCounter("cmd_gat_misses_token")
	MetricCmdGatMissesTokenL1    = metrics.AddCounter("cmd_gat_misses_token_l1")
	MetricCmdGatMissesTokenL2    = metrics.AddCounter("cmd_gat_misses_token_l2")
)

func readResponseHeader(r *bufio.Reader) (binprot.ResponseHeader, error) {
	resHeader, err := binprot.ReadResponseHeader(r)
	if err != nil {
		return binprot.ResponseHeader{}, err
	}

	if err := binprot.DecodeError(resHeader); err != nil {
		binprot.PutResponseHeader(resHeader)
		return resHeader, err
	}

	binprot.PutResponseHeader(resHeader)
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

func (h Handler) Set(cmd common.SetRequest) error {
	return h.realHandleSet(cmd, common.RequestSet)
}

func (h Handler) Add(cmd common.SetRequest) error {
	return h.realHandleSet(cmd, common.RequestAdd)
}

func (h Handler) Replace(cmd common.SetRequest) error {
	return h.realHandleSet(cmd, common.RequestReplace)
}

func (h Handler) realHandleSet(cmd common.SetRequest, reqType common.RequestType) error {

	// Specialized chunk reader to make the code here much simpler
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

	// Write metadata key
	// TODO: should there be a unique flags value for chunked data?
	switch reqType {
	case common.RequestSet:
		if err := binprot.WriteSetCmd(h.rw.Writer, metaKey, cmd.Flags, cmd.Exptime, metadataSize); err != nil {
			return err
		}
	case common.RequestAdd:
		if err := binprot.WriteAddCmd(h.rw.Writer, metaKey, cmd.Flags, cmd.Exptime, metadataSize); err != nil {
			return err
		}
	case common.RequestReplace:
		if err := binprot.WriteReplaceCmd(h.rw.Writer, metaKey, cmd.Flags, cmd.Exptime, metadataSize); err != nil {
			return err
		}
	default:
		// I know. It's all wrong. By rights we shouldn't even be here. But we are.
		panic("Unrecognized request type in realHandleSet!")
	}

	// Write value
	if err := binary.Write(h.rw, binary.BigEndian, metaData); err != nil {
		return err
	}
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, metadataSize)

	if err := h.rw.Flush(); err != nil {
		return err
	}

	// Read server's response
	resHeader, err := readResponseHeader(h.rw.Reader)
	if err != nil {
		// Discard response body
		n, ioerr := h.rw.Discard(int(resHeader.TotalBodyLength))
		metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
		if ioerr != nil {
			return ioerr
		}

		// For Add and Replace, the error here will be common.ErrKeyExists or common.ErrKeyNotFound
		// respectively. For each, this is the right response to send to the requestor. The error
		// here is overloaded because it would signal a true error for sets, but a normal "error"
		// response for Add and Replace.
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
		n, err := h.rw.Write(token[:])
		metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))
		if err != nil {
			return err
		}
		// Write value
		n2, err := io.Copy(h.rw.Writer, limChunkReader)
		metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n2))
		if err != nil {
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
			if err == common.ErrNoMem {
				metrics.IncCounter(MetricCmdSetErrorsOOM)
			}
			// Reset the ReadWriter to prevent sending garbage to memcached
			// otherwise we get disconnected. This is last-ditch and probably won't help. We should
			// probably just disconnect and reconnect to clear OS buffers
			h.reset()

			// Discard repsonse body
			n, ioerr := h.rw.Discard(int(resHeader.TotalBodyLength))
			metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
			if ioerr != nil {
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
		missResponse := common.GetResponse{
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
				metrics.IncCounter(MetricCmdGetMissesMeta)
				dataOut <- missResponse
				continue outer
			}

			errorOut <- err
			return
		}

		missResponse.Flags = metaData.OrigFlags

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
		chunk := 0
		miss := false
		var lastErr error

		for {
			opcodeNoop, err := getLocalIntoBuf(rw.Reader, metaData, tokenBuf, dataBuf, chunk, int(metaData.ChunkSize))
			if err != nil {
				if err == common.ErrKeyNotFound {
					if !miss {
						metrics.IncCounter(MetricCmdGetMissesChunk)
						miss = true
					}
					continue
				} else {
					lastErr = err
				}
			}

			if opcodeNoop {
				break
			}

			if !bytes.Equal(metaData.Token[:], tokenBuf) {
				//fmt.Println(id, "Get miss because of invalid chunk token. Cmd:", cmd)
				//fmt.Printf("Expected: %v\n", metaData.Token)
				//fmt.Printf("Got:      %v\n", tokenBuf)
				if !miss {
					metrics.IncCounter(MetricCmdGetMissesToken)
					miss = true
				}
			}

			chunk++
		}

		if lastErr != nil {
			errorOut <- lastErr
			return
		}
		if miss {
			//fmt.Println("Get miss because of missing chunk")
			dataOut <- missResponse
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

func (h Handler) GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error) {
	panic("GetE not supported in Rend")
}

func (h Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	missResponse := common.GetResponse{
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
			metrics.IncCounter(MetricCmdGatMissesMeta)
			return missResponse, nil
		}

		return common.GetResponse{}, err
	}

	missResponse.Flags = metaData.OrigFlags

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
	chunk := 0
	miss := false
	var lastErr error

	for {
		opcodeNoop, err := getLocalIntoBuf(h.rw.Reader, metaData, tokenBuf, dataBuf, chunk, int(metaData.ChunkSize))
		if err != nil {
			if err == common.ErrKeyNotFound {
				if !miss {
					metrics.IncCounter(MetricCmdGatMissesChunk)
					miss = true
				}
				continue
			} else {
				lastErr = err
			}
		}

		if opcodeNoop {
			break
		}

		if !bytes.Equal(metaData.Token[:], tokenBuf) {
			//fmt.Println(id, "GAT miss because of invalid chunk token. Cmd:", cmd)
			//fmt.Printf("Expected: %v\n", metaData.Token)
			//fmt.Printf("Got:      %v\n", tokenBuf)
			if !miss {
				metrics.IncCounter(MetricCmdGatMissesToken)
				miss = true
			}
		}

		chunk++
	}

	if lastErr != nil {
		return common.GetResponse{}, lastErr
	}
	if miss {
		//fmt.Println("GAT miss because of missing chunk")
		return missResponse, nil
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
			metrics.IncCounter(MetricCmdDeleteMissesMeta)
		}
		return err
	}

	// Delete metadata first
	if err := binprot.WriteDeleteCmd(h.rw.Writer, metaKey); err != nil {
		return err
	}
	if err := simpleCmdLocal(h.rw, true); err != nil {
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(MetricCmdDeleteMissesMeta)
		}
		return err
	}

	// Then delete data chunks
	for i := 0; i < int(metaData.NumChunks); i++ {
		chunkKey := chunkKey(cmd.Key, i)
		if err := binprot.WriteDeleteCmd(h.rw.Writer, chunkKey); err != nil {
			return err
		}
	}

	if err := h.rw.Flush(); err != nil {
		return err
	}

	miss := false
	for i := 0; i < int(metaData.NumChunks); i++ {
		if err := simpleCmdLocal(h.rw, false); err != nil {
			if err == common.ErrKeyNotFound && !miss {
				metrics.IncCounter(MetricCmdDeleteMissesChunk)
				miss = true
			}
		}
	}

	if miss {
		return common.ErrKeyNotFound
	}

	return nil
}

func (h Handler) Touch(cmd common.TouchRequest) error {
	// read metadata
	// for 0 to metadata.numChunks
	//  touch item
	// touch metadata

	// We get the metadata and not GAT it in case the key is *just* about to expire.
	// In this case if a chunk expires during the operation, we fail the touch instead of
	// leaving a key in an inconsistent state where the metadata lives on and the data is
	// incomplete. The metadata is touched last to make sure the data exists first.
	metaKey, metaData, err := getMetadata(h.rw, cmd.Key)

	if err != nil {
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(MetricCmdTouchMissesMeta)
		}

		return err
	}

	// First touch all the chunks as a batch
	for i := 0; i < int(metaData.NumChunks); i++ {
		chunkKey := chunkKey(cmd.Key, i)
		if err := binprot.WriteTouchCmd(h.rw.Writer, chunkKey, cmd.Exptime); err != nil {
			return err
		}
	}

	if err := h.rw.Flush(); err != nil {
		return err
	}

	miss := false
	for i := 0; i < int(metaData.NumChunks); i++ {
		if err := simpleCmdLocal(h.rw, false); err != nil {
			if err == common.ErrKeyNotFound && !miss {
				metrics.IncCounter(MetricCmdTouchMissesChunk)
				miss = true
			}
		}
	}

	if miss {
		return common.ErrKeyNotFound
	}

	// Then touch the metadata
	if err := binprot.WriteTouchCmd(h.rw.Writer, metaKey, cmd.Exptime); err != nil {
		return err
	}
	if err := simpleCmdLocal(h.rw, true); err != nil {
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(MetricCmdTouchMissesMeta)
		}
		return err
	}

	return nil
}
