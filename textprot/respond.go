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
	"fmt"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
)

type TextResponder struct {
	writer *bufio.Writer
}

func NewTextResponder(writer *bufio.Writer) TextResponder {
	return TextResponder{
		writer: writer,
	}
}

func (t TextResponder) Set(opaque uint32, quiet bool) error {
	return t.resp("STORED")
}

func (t TextResponder) Add(opaque uint32, quiet bool) error {
	return t.resp("STORED")
}

func (t TextResponder) Replace(opaque uint32, quiet bool) error {
	return t.resp("STORED")
}

func (t TextResponder) Get(response common.GetResponse) error {
	if response.Miss {
		// A miss is a no-op in the text world
		return nil
	}

	// Write data out to client
	// [VALUE <key> <flags> <bytes>\r\n
	// <data block>\r\n]*
	// END\r\n
	n, err := fmt.Fprintf(t.writer, "VALUE %s %d %d\r\n", response.Key, response.Flags, len(response.Data))
	metrics.IncCounterBy(common.MetricBytesWrittenRemote, uint64(n))
	if err != nil {
		return err
	}

	n, err = t.writer.Write(response.Data)
	metrics.IncCounterBy(common.MetricBytesWrittenRemote, uint64(n))
	if err != nil {
		return err
	}

	n, err = t.writer.WriteString("\r\n")
	metrics.IncCounterBy(common.MetricBytesWrittenRemote, uint64(n))
	if err != nil {
		return err
	}

	t.writer.Flush()
	return nil
}

func (t TextResponder) GetEnd(opaque uint32, noopEnd bool) error {
	return t.resp("END")
}

func (t TextResponder) GetE(response common.GetEResponse) error {
	panic("GetE command in text protocol")
}

func (t TextResponder) GAT(response common.GetResponse) error {
	// There's two options here.
	// 1) panic() because this is never supposed to be called
	// 2) Respond as a normal get
	//
	// I chose to panic, since this means we are in a bad state.
	// The text parser will never return a GAT command because
	// it does not exist in the text protocol.
	panic("GAT command in text protocol")
}

func (t TextResponder) Delete(opaque uint32) error {
	return t.resp("DELETED")
}

func (t TextResponder) Touch(opaque uint32) error {
	return t.resp("TOUCHED")
}

func (t TextResponder) Noop(opaque uint32) error {
	return t.resp("Yep, it works.")
}

func (t TextResponder) Quit(opaque uint32, quiet bool) error {
	if !quiet {
		return t.resp("Bye")
	}
	return nil
}

func (t TextResponder) Version(opaque uint32) error {
	return t.resp("VERSION " + common.VersionString)
}

func (t TextResponder) Error(opaque uint32, reqType common.RequestType, err error) error {
	switch err {
	case common.ErrKeyNotFound:
		return t.resp("NOT_FOUND")
	case common.ErrKeyExists:
		return t.resp("EXISTS")
	case common.ErrItemNotStored:
		return t.resp("NOT_STORED")
	case common.ErrValueTooBig:
		fallthrough
	case common.ErrInvalidArgs:
		return t.resp("CLIENT_ERROR bad command line")
	case common.ErrBadIncDecValue:
		return t.resp("CLIENT_ERROR invalid numeric delta argument")
	case common.ErrAuth:
		return t.resp("CLIENT_ERROR")
	case common.ErrUnknownCmd:
		fallthrough
	case common.ErrNoMem:
		fallthrough
	default:
		return t.resp(err.Error())
	}
}

func (t TextResponder) resp(s string) error {
	n, err := fmt.Fprintf(t.writer, s+"\r\n")
	metrics.IncCounterBy(common.MetricBytesWrittenRemote, uint64(n))
	if err != nil {
		return err
	}

	return t.writer.Flush()
}
