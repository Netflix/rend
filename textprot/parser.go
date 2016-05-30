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

package textprot

import (
	"bufio"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
)

type TextParser struct {
	reader *bufio.Reader
}

func NewTextParser(reader *bufio.Reader) TextParser {
	return TextParser{
		reader: reader,
	}
}

func (t TextParser) Parse() (common.Request, common.RequestType, error) {
	data, err := t.reader.ReadString('\n')
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(len(data)))

	if err != nil {
		if err == io.EOF {
			log.Println("Connection closed")
		} else {
			log.Printf("Error while reading text command line: %s\n", err.Error())
		}
		return nil, common.RequestUnknown, err
	}

	clParts := strings.Split(strings.TrimSpace(data), " ")

	switch clParts[0] {
	case "set":
		return setRequest(t.reader, clParts, common.RequestSet)

	case "add":
		return setRequest(t.reader, clParts, common.RequestAdd)

	case "replace":
		return setRequest(t.reader, clParts, common.RequestReplace)

	case "get":
		if len(clParts) < 2 {
			return nil, common.RequestGet, common.ErrBadRequest
		}

		var keys [][]byte
		for _, key := range clParts[1:] {
			keys = append(keys, []byte(key))
		}

		opaques := make([]uint32, len(keys))
		quiet := make([]bool, len(keys))

		return common.GetRequest{
			Keys:    keys,
			Opaques: opaques,
			Quiet:   quiet,
			NoopEnd: false,
		}, common.RequestGet, nil

	case "delete":
		if len(clParts) != 2 {
			return nil, common.RequestDelete, common.ErrBadRequest
		}

		return common.DeleteRequest{
			Key:    []byte(clParts[1]),
			Opaque: uint32(0),
		}, common.RequestDelete, nil

	// TODO: Error handling for invalid cmd line
	case "touch":
		if len(clParts) != 3 {
			return nil, common.RequestTouch, common.ErrBadRequest
		}

		key := []byte(clParts[1])

		exptime, err := strconv.ParseUint(strings.TrimSpace(clParts[2]), 10, 32)
		if err != nil {
			log.Printf("Error parsing ttl for touch command: %s\n", err.Error())
			return nil, common.RequestSet, common.ErrBadRequest
		}

		return common.TouchRequest{
			Key:     key,
			Exptime: uint32(exptime),
			Opaque:  uint32(0),
		}, common.RequestTouch, nil

	case "noop":
		if len(clParts) != 1 {
			return nil, common.RequestNoop, common.ErrBadRequest
		}
		return common.NoopRequest{
			Opaque: 0,
		}, common.RequestNoop, nil

	case "quit":
		if len(clParts) != 1 {
			return nil, common.RequestQuit, common.ErrBadRequest
		}
		return common.QuitRequest{
			Opaque: 0,
			Quiet:  false,
		}, common.RequestQuit, nil

	case "version":
		if len(clParts) != 1 {
			return nil, common.RequestQuit, common.ErrBadRequest
		}
		return common.VersionRequest{
			Opaque: 0,
		}, common.RequestVersion, nil

	default:
		return nil, common.RequestUnknown, nil
	}
}

func setRequest(r *bufio.Reader, clParts []string, reqType common.RequestType) (common.SetRequest, common.RequestType, error) {
	// sanity check
	if len(clParts) != 5 {
		return common.SetRequest{}, reqType, common.ErrBadRequest
	}

	key := []byte(clParts[1])

	flags, err := strconv.ParseUint(strings.TrimSpace(clParts[2]), 10, 32)
	if err != nil {
		log.Printf("Error parsing flags for set/add/replace command: %s\n", err.Error())
		return common.SetRequest{}, reqType, common.ErrBadFlags
	}

	exptime, err := strconv.ParseUint(strings.TrimSpace(clParts[3]), 10, 32)
	if err != nil {
		log.Printf("Error parsing ttl for set/add/replace command: %s\n", err.Error())
		return common.SetRequest{}, reqType, common.ErrBadExptime
	}

	length, err := strconv.ParseUint(strings.TrimSpace(clParts[4]), 10, 32)
	if err != nil {
		log.Printf("Error parsing length for set/add/replace command: %s\n", err.Error())
		return common.SetRequest{}, reqType, common.ErrBadLength
	}

	// Read in data
	dataBuf := make([]byte, length)
	n, err := io.ReadAtLeast(r, dataBuf, int(length))
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(n))
	if err != nil {
		return common.SetRequest{}, reqType, common.ErrInternal
	}

	// Consume the last two bytes "\r\n"
	r.ReadString(byte('\n'))
	metrics.IncCounterBy(common.MetricBytesReadRemote, 2)

	return common.SetRequest{
		Key:     key,
		Flags:   uint32(flags),
		Exptime: uint32(exptime),
		Opaque:  uint32(0),
		Data:    dataBuf,
	}, reqType, nil
}
