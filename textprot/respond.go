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

import "bufio"
import "fmt"

import "github.com/netflix/rend/common"

type TextResponder struct {
	writer *bufio.Writer
}

func NewTextResponder(writer *bufio.Writer) TextResponder {
	return TextResponder{
		writer: writer,
	}
}

func (t TextResponder) Set() error {
	// TODO: Error handling for less bytes
	//numWritten, err := writer.WriteString("STORED\r\n")
	_, err := t.writer.WriteString("STORED\r\n")
	if err != nil {
		return err
	}

	err = t.writer.Flush()
	if err != nil {
		return err
	}

	return nil
}

func (t TextResponder) Get(response common.GetResponse) error {
	// Write data out to client
	// [VALUE <key> <flags> <bytes>\r\n
	// <data block>\r\n]*
	// END\r\n
	_, err := fmt.Fprintf(t.writer, "VALUE %s %d %d\r\n",
		response.Key, response.Metadata.OrigFlags, response.Metadata.Length)
	if err != nil {
		return err
	}

	_, err = t.writer.Write(response.Data)
	if err != nil {
		return err
	}

	_, err = t.writer.WriteString("\r\n")
	if err != nil {
		return err
	}

	t.writer.Flush()
	return nil
}

func (t TextResponder) GetMiss(response common.GetResponse) error {
	// A miss is a no-op in the text world
	return nil
}

func (t TextResponder) GetEnd(noopEnd bool) error {
	_, err := fmt.Fprintf(t.writer, "END\r\n")
	if err != nil {
		return err
	}

	t.writer.Flush()
	return nil
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

func (t TextResponder) GATMiss(response common.GetResponse) error {
	panic("GAT command in text protocol")
}

func (t TextResponder) Delete() error {
	_, err := fmt.Fprintf(t.writer, "DELETED\r\n")
	if err != nil {
		return err
	}

	t.writer.Flush()
	return nil
}

func (t TextResponder) Touch() error {
	_, err := fmt.Fprintf(t.writer, "TOUCHED\r\n")
	if err != nil {
		return err
	}

	t.writer.Flush()
	return nil
}

func (t TextResponder) Error(err error) error {

	switch err {
	case common.ErrKeyNotFound:
		_, err = fmt.Fprintf(t.writer, "NOT_FOUND\r\n")
	case common.ErrKeyExists:
		_, err = fmt.Fprintf(t.writer, "EXISTS\r\n")
	case common.ErrItemNotStored:
		_, err = fmt.Fprintf(t.writer, "NOT_STORED\r\n")
	case common.ErrValueTooBig:
	case common.ErrInvalidArgs:
		_, err = fmt.Fprintf(t.writer, "CLIENT_ERROR bad command line\r\n")
	case common.ErrBadIncDecValue:
		_, err = fmt.Fprintf(t.writer, "CLIENT_ERROR invalid numeric delta argument\r\n")
	case common.ErrAuth:
		_, err = fmt.Fprintf(t.writer, "CLIENT_ERROR\r\n")
	case common.ErrUnknownCmd:
	case common.ErrNoMem:
	default:
		_, err = fmt.Fprintf(t.writer, "ERROR\r\n")
	}

	if err != nil {
		return err
	}
	t.writer.Flush()
	return nil
}
