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
	"io"
	"math"
	"time"

	"github.com/netflix/rend/binprot"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
)

var (
	MetricCmdSetErrorsOOM   = metrics.AddCounter("cmd_set_errors_oom", nil)
	MetricCmdSetErrorsOOML1 = metrics.AddCounter("cmd_set_errors_oom_l1", nil)
	MetricCmdSetErrorsOOML2 = metrics.AddCounter("cmd_set_errors_oom_l2", nil)

	MetricCmdTouchMissesMeta    = metrics.AddCounter("cmd_touch_misses_meta", nil)
	MetricCmdTouchMissesMetaL1  = metrics.AddCounter("cmd_touch_misses_meta_l1", nil)
	MetricCmdTouchMissesMetaL2  = metrics.AddCounter("cmd_touch_misses_meta_l2", nil)
	MetricCmdTouchMissesChunk   = metrics.AddCounter("cmd_touch_misses_chunk", nil)
	MetricCmdTouchMissesChunkL1 = metrics.AddCounter("cmd_touch_misses_chunk_l1", nil)
	MetricCmdTouchMissesChunkL2 = metrics.AddCounter("cmd_touch_misses_chunk_l2", nil)

	MetricCmdTouchMetaSet            = metrics.AddCounter("cmd_touch_meta_set", nil)
	MetricCmdTouchMetaSetL1          = metrics.AddCounter("cmd_touch_meta_set_l1", nil)
	MetricCmdTouchMetaSetL2          = metrics.AddCounter("cmd_touch_meta_set_l2", nil)
	MetricCmdTouchMetaSetErrors      = metrics.AddCounter("cmd_touch_meta_set_errors", nil)
	MetricCmdTouchMetaSetErrorsL1    = metrics.AddCounter("cmd_touch_meta_set_errors_l1", nil)
	MetricCmdTouchMetaSetErrorsL2    = metrics.AddCounter("cmd_touch_meta_set_errors_l2", nil)
	MetricCmdTouchMetaSetSuccesses   = metrics.AddCounter("cmd_touch_meta_set_successes", nil)
	MetricCmdTouchMetaSetSuccessesL1 = metrics.AddCounter("cmd_touch_meta_set_successes_l1", nil)
	MetricCmdTouchMetaSetSuccessesL2 = metrics.AddCounter("cmd_touch_meta_set_successes_l2", nil)

	MetricCmdDeleteMissesMeta    = metrics.AddCounter("cmd_delete_misses_meta", nil)
	MetricCmdDeleteMissesMetaL1  = metrics.AddCounter("cmd_delete_misses_meta_l1", nil)
	MetricCmdDeleteMissesMetaL2  = metrics.AddCounter("cmd_delete_misses_meta_l2", nil)
	MetricCmdDeleteMissesChunk   = metrics.AddCounter("cmd_delete_misses_chunk", nil)
	MetricCmdDeleteMissesChunkL1 = metrics.AddCounter("cmd_delete_misses_chunk_l1", nil)
	MetricCmdDeleteMissesChunkL2 = metrics.AddCounter("cmd_delete_misses_chunk_l2", nil)

	MetricCmdGetMissesMeta    = metrics.AddCounter("cmd_get_misses_meta", nil)
	MetricCmdGetMissesMetaL1  = metrics.AddCounter("cmd_get_misses_meta_l1", nil)
	MetricCmdGetMissesMetaL2  = metrics.AddCounter("cmd_get_misses_meta_l2", nil)
	MetricCmdGetMissesChunk   = metrics.AddCounter("cmd_get_misses_chunk", nil)
	MetricCmdGetMissesChunkL1 = metrics.AddCounter("cmd_get_misses_chunk_l1", nil)
	MetricCmdGetMissesChunkL2 = metrics.AddCounter("cmd_get_misses_chunk_l2", nil)
	MetricCmdGetMissesToken   = metrics.AddCounter("cmd_get_misses_token", nil)
	MetricCmdGetMissesTokenL1 = metrics.AddCounter("cmd_get_misses_token_l1", nil)
	MetricCmdGetMissesTokenL2 = metrics.AddCounter("cmd_get_misses_token_l2", nil)

	MetricCmdGatMissesMeta    = metrics.AddCounter("cmd_gat_misses_meta", nil)
	MetricCmdGatMissesMetaL1  = metrics.AddCounter("cmd_gat_misses_meta_l1", nil)
	MetricCmdGatMissesMetaL2  = metrics.AddCounter("cmd_gat_misses_meta_l2", nil)
	MetricCmdGatMissesChunk   = metrics.AddCounter("cmd_gat_misses_chunk", nil)
	MetricCmdGatMissesChunkL1 = metrics.AddCounter("cmd_gat_misses_chunk_l1", nil)
	MetricCmdGatMissesChunkL2 = metrics.AddCounter("cmd_gat_misses_chunk_l2", nil)
	MetricCmdGatMissesToken   = metrics.AddCounter("cmd_gat_misses_token", nil)
	MetricCmdGatMissesTokenL1 = metrics.AddCounter("cmd_gat_misses_token_l1", nil)
	MetricCmdGatMissesTokenL2 = metrics.AddCounter("cmd_gat_misses_token_l2", nil)

	MetricCmdAppendMissesMeta    = metrics.AddCounter("cmd_append_misses_meta", nil)
	MetricCmdAppendMissesMetaL1  = metrics.AddCounter("cmd_append_misses_meta_l1", nil)
	MetricCmdAppendMissesMetaL2  = metrics.AddCounter("cmd_append_misses_meta_l2", nil)
	MetricCmdAppendMissesChunk   = metrics.AddCounter("cmd_append_misses_chunk", nil)
	MetricCmdAppendMissesChunkL1 = metrics.AddCounter("cmd_append_misses_chunk_l1", nil)
	MetricCmdAppendMissesChunkL2 = metrics.AddCounter("cmd_append_misses_chunk_l2", nil)
	MetricCmdAppendMissesToken   = metrics.AddCounter("cmd_append_misses_token", nil)
	MetricCmdAppendMissesTokenL1 = metrics.AddCounter("cmd_append_misses_token_l1", nil)
	MetricCmdAppendMissesTokenL2 = metrics.AddCounter("cmd_append_misses_token_l2", nil)

	MetricCmdPrependMissesMeta    = metrics.AddCounter("cmd_prepend_misses_meta", nil)
	MetricCmdPrependMissesMetaL1  = metrics.AddCounter("cmd_prepend_misses_meta_l1", nil)
	MetricCmdPrependMissesMetaL2  = metrics.AddCounter("cmd_prepend_misses_meta_l2", nil)
	MetricCmdPrependMissesChunk   = metrics.AddCounter("cmd_prepend_misses_chunk", nil)
	MetricCmdPrependMissesChunkL1 = metrics.AddCounter("cmd_prepend_misses_chunk_l1", nil)
	MetricCmdPrependMissesChunkL2 = metrics.AddCounter("cmd_prepend_misses_chunk_l2", nil)
	MetricCmdPrependMissesToken   = metrics.AddCounter("cmd_prepend_misses_token", nil)
	MetricCmdPrependMissesTokenL1 = metrics.AddCounter("cmd_prepend_misses_token_l1", nil)
	MetricCmdPrependMissesTokenL2 = metrics.AddCounter("cmd_prepend_misses_token_l2", nil)

	progStart = time.Now().Unix()
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
	return h.handleSetCommon(cmd, common.RequestSet)
}

func (h Handler) Add(cmd common.SetRequest) error {
	return h.handleSetCommon(cmd, common.RequestAdd)
}

func (h Handler) Replace(cmd common.SetRequest) error {
	return h.handleSetCommon(cmd, common.RequestReplace)
}

const (
	// TODO: Make configurable at command line
	// might be a constructor argument
	// Slab 12, ~1KB per chunk
	chunkMaxSize = 1184

	// Format of headers in memcached:
	//
	// Key size + 1 + Header (Flags + Key + 2 bytes (\r\n) + 4 bytes (2 spaces and 1 \r)) + Chunk Size + CAS Size + 3
	// + 1 // Space
	// + 4 // Flags
	// + 4 // Key
	// + 2 // /r/n
	// + 4 // 2 spaces and 1 \r
	// + 48 // Header Size
	// + 8 // CAS
	// 4 because we will suffix _00, _01 ... _99, 68 is the size of the memcached header
	// we kind of assume there will only be at most 999 chunks, but if there's more we can up the chunk max size
	// tokenSize
	//
	// TODO: Double check. 71 is currently used in the EVCache client but 67 works, so 4 less bytes overhead
	chunkOverhead = 67 + 4
)

func chunkSize(keylen int) (dataSize, fullSize uint32) {
	fullSize = uint32(chunkMaxSize - chunkOverhead - keylen)
	dataSize = fullSize - tokenSize
	return
}

// The maximum differential TTL allowed by memcached
const realTimeMaxDelta = 60 * 60 * 24 * 30

// Takes a TTL in seconds and returns the unix time in seconds when the item will expire.
func exptime(ttl uint32) (exp uint32, expired bool) {
	// zero is the special forever case
	if ttl == 0 {
		return 0, false
	}

	now := uint32(time.Now().Unix())

	// The memcached protocol has a... "quirk" where any expiration time over 30
	// days is considered to be a unix timestamp.
	if ttl > realTimeMaxDelta {
		return ttl, (ttl < now)
	}

	// otherwise, this is a normal differential TTL
	return now + ttl, false
}

func (h Handler) handleSetCommon(cmd common.SetRequest, reqType common.RequestType) error {
	exp, expired := exptime(cmd.Exptime)
	if expired {
		return nil
	}

	// Specialized chunk reader to make the code here much simpler
	dataSize, fullSize := chunkSize(len(cmd.Key))
	limChunkReader := newChunkLimitedReader(bytes.NewBuffer(cmd.Data), int64(dataSize), int64(len(cmd.Data)))
	numChunks := int(math.Ceil(float64(len(cmd.Data)) / float64(dataSize)))
	token := <-tokens

	metaKey := metaKey(cmd.Key)
	metaData := metadata{
		Length:    uint32(len(cmd.Data)),
		OrigFlags: cmd.Flags,
		NumChunks: uint32(numChunks),
		ChunkSize: dataSize,
		Token:     token,
		Instime:   uint32(time.Now().Unix()),
		Exptime:   exp,
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
	writeMetadata(h.rw, metaData)

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
		if err := binprot.WriteSetCmd(h.rw.Writer, key, cmd.Flags, cmd.Exptime, fullSize); err != nil {
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

func (h Handler) Append(cmd common.SetRequest) error {
	return h.handleAppendPrependCommon(cmd, common.RequestAppend)
}

func (h Handler) Prepend(cmd common.SetRequest) error {
	return h.handleAppendPrependCommon(cmd, common.RequestPrepend)
}

func (h Handler) handleAppendPrependCommon(cmd common.SetRequest, reqType common.RequestType) error {
	// read data, (ap|pre)pend, write out

	switch reqType {
	case common.RequestAppend, common.RequestPrepend:
	default:
		// I know. It’s all wrong. By rights we shouldn’t even be here. But we are.
		panic("Bad request type in appendPrependCommon!")
	}

	_, metaData, err := getMetadata(h.rw, cmd.Key)
	if err != nil {
		if err == common.ErrKeyNotFound {
			switch reqType {
			case common.RequestAppend:
				metrics.IncCounter(MetricCmdAppendMissesMeta)
			case common.RequestPrepend:
				metrics.IncCounter(MetricCmdPrependMissesMeta)
			}

			return common.ErrKeyNotFound
		}

		return err
	}

	// Write all the get commands before reading
	cmdSize := int(metaData.NumChunks)*(len(cmd.Key)+4 /* key suffix */ +binprot.ReqHeaderLen) + binprot.ReqHeaderLen /* for the noop */
	cmdbuf := bytes.NewBuffer(make([]byte, 0, cmdSize))
	for i := 0; i < int(metaData.NumChunks); i++ {
		chunkKey := chunkKey(cmd.Key, i)
		binprot.WriteGetQCmd(cmdbuf, chunkKey)
	}
	binprot.WriteNoopCmd(cmdbuf)

	// Write everyhing and flush to ensure it's sent
	if _, err := h.rw.ReadFrom(cmdbuf); err != nil {
		return err
	}
	if err := h.rw.Flush(); err != nil {
		return err
	}

	dataBuf := make([]byte, int(metaData.Length))
	tokenBuf := make([]byte, tokenSize)

	// Now that all the headers are sent, start reading in the data chunks. We read until the header
	// for the Noop command comes back, keeping track of how many chunks are read. This means that
	// there is no fast fail when a chunk is missing, but all the data is read in so there's no
	// problem with unread, buffered data that should have been discarded. If the number of chunks
	// doesn't match, we throw away the data and call it a miss.
	var chunk int
	var miss bool
	var lastErr error

	for {
		opcodeNoop, err := getLocalIntoBuf(h.rw.Reader, metaData, tokenBuf, dataBuf, chunk, int(metaData.ChunkSize))
		if err != nil {
			if err == common.ErrKeyNotFound {
				if !miss {
					switch reqType {
					case common.RequestAppend:
						metrics.IncCounter(MetricCmdAppendMissesChunk)
					case common.RequestPrepend:
						metrics.IncCounter(MetricCmdPrependMissesChunk)
					}
					miss = true
				}
				continue
			}

			lastErr = err
		}

		if opcodeNoop {
			break
		}

		if !bytes.Equal(metaData.Token[:], tokenBuf) {
			if !miss {
				switch reqType {
				case common.RequestAppend:
					metrics.IncCounter(MetricCmdAppendMissesToken)
				case common.RequestPrepend:
					metrics.IncCounter(MetricCmdPrependMissesToken)
				}
				miss = true
			}
		}

		chunk++
	}

	if lastErr != nil {
		return lastErr
	}
	if miss {
		//fmt.Println("Get miss because of missing chunk")
		return common.ErrKeyNotFound
	}

	// append or prepend, the meat of the request
	if reqType == common.RequestAppend {
		dataBuf = append(dataBuf, cmd.Data...)
	} else {
		dataBuf = append(cmd.Data, dataBuf...)
	}

	// now put it again. Insert time won't be exact, here, but the expiration is still valid
	setcmd := common.SetRequest{
		Key:     cmd.Key,
		Data:    dataBuf,
		Flags:   metaData.OrigFlags,
		Exptime: metaData.Exptime,
	}
	return h.handleSetCommon(setcmd, common.RequestSet)
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

		cmdSize := int(metaData.NumChunks)*(len(key)+4 /* key suffix */ +binprot.ReqHeaderLen) + binprot.ReqHeaderLen /* for the noop */
		cmdbuf := bytes.NewBuffer(make([]byte, 0, cmdSize))
		// Write all the get commands before reading
		for i := 0; i < int(metaData.NumChunks); i++ {
			chunkKey := chunkKey(key, i)
			// bytes.Buffer doesn't error
			binprot.WriteGetQCmd(cmdbuf, chunkKey)
		}

		// The final command must be Get or Noop to guarantee a response
		// We use Noop to make coding easier, but it's (very) slightly less efficient
		// since we send 24 extra bytes in each direction
		// bytes.Buffer doesn't error
		binprot.WriteNoopCmd(cmdbuf)

		// bufio's ReadFrom will end up doing an io.Copy(cmdbuf, socket), which is more
		// efficient than writing directly into the bufio or using cmdbuf.WriteTo(rw)
		if _, err := rw.ReadFrom(cmdbuf); err != nil {
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
	// Being minimalist, not lazy. The chunked handler is not meant to be used with a
	// backing store that supports the GetE protocol extension. It would be a waste of
	// time and effort to support it here if it would "never" be used. It will be added
	// as soon as a case for it exists.
	//
	// The GetE extension is an addition that only rend supports. This chunked handler
	// is pretty explicitly for talking to memcached since it is a complex workaround
	// for pathological behavior when data size rapidly changes that only happens in
	// memcached. The chunked handler will not work well with the L2 the EVCache team
	// uses.
	panic("GetE not supported in Rend chunked mode")
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

	// Overwrite the metadata with the new expiration time
	metrics.IncCounter(MetricCmdTouchMetaSet)
	metaData.Exptime, _ = exptime(cmd.Exptime)
	if err := binprot.WriteSetCmd(h.rw.Writer, metaKey, metaData.OrigFlags, cmd.Exptime, metadataSize); err != nil {
		return err
	}

	// Write value
	writeMetadata(h.rw, metaData)
	if err := h.rw.Flush(); err != nil {
		return err
	}

	// Read server's response
	resHeader, err := readResponseHeader(h.rw.Reader)
	if err != nil {
		metrics.IncCounter(MetricCmdTouchMetaSetErrors)
		// Discard response body
		n, ioerr := h.rw.Discard(int(resHeader.TotalBodyLength))
		metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
		if ioerr != nil {
			return ioerr
		}
		return err
	}
	metrics.IncCounter(MetricCmdTouchMetaSetSuccesses)

	return nil
}
