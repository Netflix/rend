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
import "encoding/binary"
import "io"

import "github.com/netflix/rend/binprot"

func getAndTouchMetadata(rw *bufio.ReadWriter, key []byte, exptime uint32) ([]byte, metadata, error) {
	metaKey := metaKey(key)
	if err := binprot.WriteGATCmd(metaKey, exptime); err != nil {
		return nil, metadata{}, err
	}
	metadata, err := getMetadataCommon(rw)
	return metaKey, metadata, err
}

func getMetadata(rw *bufio.ReadWriter, key []byte) ([]byte, metadata, error) {
	metaKey := metaKey(key)
	if err := binprot.WriteGetCmd(metaKey); err != nil {
		return nil, metadata{}, err
	}
	metadata, err := getMetadataCommon(rw)
	return metaKey, metadata, err
}

func getMetadataCommon(rw *bufio.ReadWriter) (metadata, error) {
	if err := rw.Flush(); err != nil {
		return metadata{}, err
	}

	resHeader, err := binprot.ReadResponseHeader(rw)
	if err != nil {
		return metadata{}, err
	}

	err = binprot.DecodeError(resHeader)
	if err != nil {
		// read in the message "Not found" after a miss
		if _, ioerr := rw.Discard(int(resHeader.TotalBodyLength)); ioerr != nil {
			return metadata{}, ioerr
		}
		return metadata{}, err
	}

	serverFlags := make([]byte, 4)
	binary.Read(rw, binary.BigEndian, &serverFlags)

	var metaData metadata
	binary.Read(rw, binary.BigEndian, &metaData)

	return metaData, nil
}

func setLocalWithToken(w *bufio.Writer, cmd []byte, token [16]byte, data io.Reader) error {
	cmd = append(cmd, token[:]...)
	return setLocal(w, cmd, data)
}

func simpleCmdLocal(rw *bufio.ReadWriter, cmd []byte) error {
	if _, err := rw.Write(cmd); err != nil {
		return err
	}

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

func getLocalIntoBuf(rw *bufio.ReadWriter, tokenBuf []byte, buf []byte, totalDataLength int) (opcodeNoop bool, err error) {
	resHeader, err := binprot.ReadResponseHeader(rw)
	if err != nil {
		return err
	}

	// it feels a bit dirty knowing about batch gets here, but it's the most logical place to put
	// a check for an opcode that signals the end of a batch get or GAT. This code is a bit too big
	// to copy-paste in multiple places.
	if resHeader.Opcode == binprot.OpcodeNoop {
		return true, nil
	}

	err = binprot.DecodeError(resHeader)
	if err != nil {
		if _, ioerr := rw.Discard(int(resHeader.TotalBodyLength)); ioerr != nil {
			return false, ioerr
		}
		return false, err
	}

	serverFlags := make([]byte, 4)
	binary.Read(rw, binary.BigEndian, &serverFlags)

	// Read in token if requested
	if tokenBuf != nil {
		if _, err := io.ReadFull(rw, tokenBuf); err != nil {
			return false, err
		}
	}

	// Read in value
	if _, err := io.ReadFull(rw, buf); err != nil {
		return false, err
	}

	// consume padding at end of chunk if needed
	if len(buf) < totalDataLength {
		if _, ioerr := rw.Discard(totalDataLength - len(buf)); ioerr != nil {
			return false, ioerr
		}
	}

	return false, nil
}
