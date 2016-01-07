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
import "github.com/netflix/rend/common"

func getAndTouchMetadata(rw *bufio.ReadWriter, key []byte, exptime uint32) ([]byte, common.Metadata, error) {
	metaKey := metaKey(key)
	metadata, err := getMetadataCommon(rw, binprot.GATCmd(metaKey, exptime))
	return metaKey, metadata, err
}

func getMetadata(rw *bufio.ReadWriter, key []byte) ([]byte, common.Metadata, error) {
	metaKey := metaKey(key)
	metadata, err := getMetadataCommon(rw, binprot.GetCmd(metaKey))
	return metaKey, metadata, err
}

func getMetadataCommon(rw *bufio.ReadWriter, getCmd []byte) (common.Metadata, error) {
	if _, err := rw.Write(getCmd); err != nil {
		return common.Metadata{}, err
	}

	if err := rw.Flush(); err != nil {
		return common.Metadata{}, err
	}

	resHeader, err := binprot.ReadResponseHeader(rw)
	if err != nil {
		return common.Metadata{}, err
	}

	err = binprot.DecodeError(resHeader)
	if err != nil {
		// read in the message "Not found" after a miss
		if _, ioerr := rw.Discard(int(resHeader.TotalBodyLength)); ioerr != nil {
			return common.Metadata{}, ioerr
		}
		return common.Metadata{}, err
	}

	serverFlags := make([]byte, 4)
	binary.Read(rw, binary.BigEndian, &serverFlags)

	var metaData common.Metadata
	binary.Read(rw, binary.BigEndian, &metaData)

	return metaData, nil
}

func setLocalWithToken(w *bufio.Writer, cmd []byte, token [16]byte, data io.Reader) error {
	cmd = append(cmd, token[:]...)
	return setLocal(w, cmd, data)
}

func setLocal(w *bufio.Writer, cmd []byte, data io.Reader) error {
	// Write header
	if _, err := w.Write(cmd); err != nil {
		return err
	}

	// Write value
	if _, err := io.Copy(w, data); err != nil {
		return err
	}

	return w.Flush()
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

func getLocalIntoBuf(rw *bufio.ReadWriter, cmd []byte, tokenBuf []byte, buf []byte, totalDataLength int) error {
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

	serverFlags := make([]byte, 4)
	binary.Read(rw, binary.BigEndian, &serverFlags)

	// Read in token if requested
	if tokenBuf != nil {
		if _, err := io.ReadFull(rw, tokenBuf); err != nil {
			return err
		}
	}

	// Read in value
	if _, err := io.ReadFull(rw, buf); err != nil {
		return err
	}

	// consume padding at end of chunk if needed
	if len(buf) < totalDataLength {
		if _, ioerr := rw.Discard(totalDataLength-len(buf)); ioerr != nil {
			return ioerr
		}
	}

	return nil
}
