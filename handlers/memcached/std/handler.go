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
	"io"

	"github.com/netflix/rend/binprot"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
)

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

func (h Handler) Set(cmd common.SetRequest) error {
	// TODO: should there be a unique flags value for regular data?
	// Write command header
	if err := binprot.WriteSetCmd(h.rw.Writer, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data))); err != nil {
		return err
	}

	// Write value
	h.rw.Write(cmd.Data)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(len(cmd.Data)))

	if err := h.rw.Flush(); err != nil {
		return err
	}

	// Read server's response
	resHeader, err := readResponseHeader(h.rw.Reader)
	if err != nil {
		// Discard request body
		/*/ only when using streaming sets
		n, ioerr := src.Discard(len(cmd.Data))
		metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(n))
		if ioerr != nil {
			return ioerr
		}*/

		// Discard response body
		n, ioerr := h.rw.Discard(int(resHeader.TotalBodyLength))
		metrics.IncCounterBy(common.MetricBytesReadLocal, uint64(n))
		if ioerr != nil {
			return ioerr
		}

		return err
	}

	return nil
}

func (h Handler) Add(cmd common.SetRequest) error {
	// TODO: should there be a unique flags value for regular data?
	// Write command header
	if err := binprot.WriteAddCmd(h.rw.Writer, cmd.Key, cmd.Flags, cmd.Exptime, uint32(len(cmd.Data))); err != nil {
		return err
	}

	// Write value
	h.rw.Write(cmd.Data)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(len(cmd.Data)))

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
		if err := binprot.WriteGetCmd(rw.Writer, key); err != nil {
			errorOut <- err
			return
		}

		data, flags, err := getLocal(rw)
		if err != nil {
			if err == common.ErrKeyNotFound {
				dataOut <- common.GetResponse{
					Miss:   true,
					Quiet:  cmd.Quiet[idx],
					Opaque: cmd.Opaques[idx],
					Flags:  flags,
					Key:    key,
					Data:   nil,
				}

				continue
			}

			errorOut <- err
			return
		}

		dataOut <- common.GetResponse{
			Miss:   false,
			Quiet:  cmd.Quiet[idx],
			Opaque: cmd.Opaques[idx],
			Flags:  flags,
			Key:    key,
			Data:   data,
		}
	}
}

func (h Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	if err := binprot.WriteGATCmd(h.rw.Writer, cmd.Key, cmd.Exptime); err != nil {
		return common.GetResponse{}, err
	}

	data, flags, err := getLocal(h.rw)
	if err != nil {
		if err == common.ErrKeyNotFound {
			//fmt.Println("GAT miss because of missing metadata. Key:", key)
			return common.GetResponse{
				Miss:   true,
				Quiet:  false,
				Opaque: cmd.Opaque,
				Flags:  flags,
				Key:    cmd.Key,
				Data:   nil,
			}, nil
		}

		return common.GetResponse{}, err
	}

	return common.GetResponse{
		Miss:   false,
		Quiet:  false,
		Opaque: cmd.Opaque,
		Flags:  flags,
		Key:    cmd.Key,
		Data:   data,
	}, nil
}

func (h Handler) Delete(cmd common.DeleteRequest) error {
	if err := binprot.WriteDeleteCmd(h.rw.Writer, cmd.Key); err != nil {
		return err
	}
	return simpleCmdLocal(h.rw)
}

func (h Handler) Touch(cmd common.TouchRequest) error {
	if err := binprot.WriteTouchCmd(h.rw.Writer, cmd.Key, cmd.Exptime); err != nil {
		return err
	}
	return simpleCmdLocal(h.rw)
}
