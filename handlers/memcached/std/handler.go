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

import "bufio"
import "io"

import "github.com/netflix/rend/binprot"
import "github.com/netflix/rend/common"

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
	// TODO: should there be a unique flags value for regular data?
	setCmd := binprot.SetCmd(cmd.Key, cmd.Flags, cmd.Exptime, cmd.Length)

	// Write command header
	h.rw.Write(setCmd)
	// Write value
	lr := io.LimitReader(src, int64(cmd.Length))
	io.Copy(h.rw, lr)

	if err := h.rw.Flush(); err != nil {
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

	return nil
}

func (h Handler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	dataOut := make(chan common.GetResponse)
	errorOut := make(chan error)
	go realHandleGet(cmd, dataOut, errorOut, h.rw)
	return dataOut, errorOut
}

func realHandleGet(cmd common.GetRequest, dataOut chan common.GetResponse, errorOut chan error, rw *bufio.ReadWriter) {
	defer close(errorOut)
	defer close(dataOut)

	for idx, key := range cmd.Keys {
		getCmd := binprot.GetCmd(key)
		data, err := getLocal(rw, getCmd)

		if err != nil {
			if err == common.ErrKeyNotFound {
				dataOut <- common.GetResponse{
					Miss:     true,
					Key:      key,
					Opaque:   cmd.Opaques[idx],
					Quiet:    cmd.Quiet[idx],
					Metadata: common.Metadata{},
					Data:     nil,
				}

				continue
			}

			errorOut <- err
			return
		}

		dataOut <- common.GetResponse{
			Miss:     false,
			Key:      key,
			Opaque:   cmd.Opaques[idx],
			Quiet:    cmd.Quiet[idx],
			Metadata: common.Metadata{},
			Data:     data,
		}
	}
}

func (h Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	getCmd := binprot.GATCmd(cmd.Key, cmd.Exptime)
	data, err := getLocal(h.rw, getCmd)

	if err != nil {
		if err == common.ErrKeyNotFound {
			//fmt.Println("GAT miss because of missing metadata. Key:", key)
			return common.GetResponse{
				Miss:     true,
				Key:      cmd.Key,
				Opaque:   cmd.Opaque,
				Quiet:    false,
				Metadata: common.Metadata{},
				Data:     nil,
			}, nil
		}

		return common.GetResponse{}, err
	}

	return common.GetResponse{
		Miss:     false,
		Key:      cmd.Key,
		Opaque:   cmd.Opaque,
		Quiet:    false,
		Metadata: common.Metadata{},
		Data:     data,
	}, nil
}

func (h Handler) Delete(cmd common.DeleteRequest) error {
	deleteCmd := binprot.DeleteCmd(cmd.Key)
	return simpleCmdLocal(h.rw, deleteCmd)
}

func (h Handler) Touch(cmd common.TouchRequest) error {
	touchCmd := binprot.TouchCmd(cmd.Key, cmd.Exptime)
	return simpleCmdLocal(h.rw, touchCmd)
}
