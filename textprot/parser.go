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
	"github.com/netflix/rend/timer"
)

type TextParser struct {
	reader *bufio.Reader
}

func NewTextParser(reader *bufio.Reader) TextParser {
	return TextParser{
		reader: reader,
	}
}

func (t TextParser) Parse() (common.Request, common.RequestType, uint64, error) {
	data, err := t.reader.ReadString('\n')
	start := timer.Now()
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(len(data)))

	if err != nil {
		if err == io.EOF {
			//log.Println("Connection closed")
		} else {
			log.Printf("Error while reading text command line: %s\n", err.Error())
		}
		return nil, common.RequestUnknown, start, err
	}

	clParts := strings.Split(strings.TrimSpace(data), " ")

	switch clParts[0] {
	case "set":
		return setRequest(t.reader, clParts, common.RequestSet, start)

	case "add":
		return setRequest(t.reader, clParts, common.RequestAdd, start)

	case "replace":
		return setRequest(t.reader, clParts, common.RequestReplace, start)

	case "append":
		return setRequest(t.reader, clParts, common.RequestAppend, start)

	case "prepend":
		return setRequest(t.reader, clParts, common.RequestPrepend, start)

	case "get":
		if len(clParts) < 2 {
			return nil, common.RequestGet, start, common.ErrBadRequest
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
		}, common.RequestGet, start, nil

	case "delete":
		if len(clParts) != 2 {
			return nil, common.RequestDelete, start, common.ErrBadRequest
		}

		return common.DeleteRequest{
			Key:    []byte(clParts[1]),
			Opaque: uint32(0),
		}, common.RequestDelete, start, nil

	// TODO: Error handling for invalid cmd line
	case "touch":
		if len(clParts) != 3 {
			return nil, common.RequestTouch, start, common.ErrBadRequest
		}

		key := []byte(clParts[1])

		exptime, err := strconv.ParseUint(strings.TrimSpace(clParts[2]), 10, 32)
		if err != nil {
			log.Printf("Error parsing ttl for touch command: %s\n", err.Error())
			return nil, common.RequestSet, start, common.ErrBadRequest
		}

		return common.TouchRequest{
			Key:     key,
			Exptime: uint32(exptime),
			Opaque:  uint32(0),
		}, common.RequestTouch, start, nil
	case "noop":
		if len(clParts) != 1 {
			return nil, common.RequestNoop, start, common.ErrBadRequest
		}
		return common.NoopRequest{
			Opaque: 0,
		}, common.RequestNoop, start, nil

	case "quit":
		if len(clParts) != 1 {
			return nil, common.RequestQuit, start, common.ErrBadRequest
		}
		return common.QuitRequest{
			Opaque: 0,
			Quiet:  false,
		}, common.RequestQuit, start, nil

	case "version":
		if len(clParts) != 1 {
			return nil, common.RequestQuit, start, common.ErrBadRequest
		}
		return common.VersionRequest{
			Opaque: 0,
		}, common.RequestVersion, start, nil

	default:
		return nil, common.RequestUnknown, start, nil
	}
}

func setRequest(r *bufio.Reader, clParts []string, reqType common.RequestType, start uint64) (common.SetRequest, common.RequestType, uint64, error) {
	// sanity check
	if len(clParts) != 5 {
		return common.SetRequest{}, reqType, start, common.ErrBadRequest
	}

	key := []byte(clParts[1])

	flags, err := strconv.ParseUint(strings.TrimSpace(clParts[2]), 10, 32)
	if err != nil {
		log.Printf("Error parsing flags for set/add/replace command: %s\n", err.Error())
		return common.SetRequest{}, reqType, start, common.ErrBadFlags
	}

	exptime, err := strconv.ParseUint(strings.TrimSpace(clParts[3]), 10, 32)
	if err != nil {
		log.Printf("Error parsing ttl for set/add/replace command: %s\n", err.Error())
		return common.SetRequest{}, reqType, start, common.ErrBadExptime
	}

	length, err := strconv.ParseUint(strings.TrimSpace(clParts[4]), 10, 32)
	if err != nil {
		log.Printf("Error parsing length for set/add/replace command: %s\n", err.Error())
		return common.SetRequest{}, reqType, start, common.ErrBadLength
	}

	// Read in data
	dataBuf := make([]byte, length)
	n, err := io.ReadAtLeast(r, dataBuf, int(length))
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(n))
	if err != nil {
		return common.SetRequest{}, reqType, start, common.ErrInternal
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
	}, reqType, start, nil
}
