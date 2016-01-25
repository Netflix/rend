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
import "encoding/binary"
import "io"

import "github.com/netflix/rend/binprot"

func simpleCmdLocal(rw *bufio.ReadWriter) error {
	if err := rw.Flush(); err != nil {
		return err
	}

	resHeader, err := binprot.ReadResponseHeader(rw)
	if err != nil {
		return err
	}

	err = binprot.DecodeError(resHeader)
	if err != nil {
		if _, ioerr := rw.Discard(int(resHeader.TotalBodyLength)); ioerr != nil {
			return ioerr
		}
		return err
	}

	// Read in the message bytes from the body
	if _, err := rw.Discard(int(resHeader.TotalBodyLength)); err != nil {
		return err
	}

	return nil
}

func getLocal(rw *bufio.ReadWriter) (data []byte, flags uint32, err error) {
	if err := rw.Flush(); err != nil {
		return nil, 0, err
	}

	resHeader, err := binprot.ReadResponseHeader(rw)
	if err != nil {
		return nil, 0, err
	}

	err = binprot.DecodeError(resHeader)
	if err != nil {
		if _, ioerr := rw.Discard(int(resHeader.TotalBodyLength)); ioerr != nil {
			return nil, 0, ioerr
		}
		return nil, 0, err
	}

	var serverFlags uint32
	binary.Read(rw, binary.BigEndian, &serverFlags)

	// total body - key - extra
	dataLen := resHeader.TotalBodyLength - uint32(resHeader.KeyLength) - uint32(resHeader.ExtraLength)
	buf := make([]byte, dataLen)

	// Read in value
	if _, err := io.ReadFull(rw, buf); err != nil {
		return nil, 0, err
	}

	return buf, serverFlags, nil
}
