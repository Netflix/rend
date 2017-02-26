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

package binprot

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/netflix/rend/common"
)

func TestUnknownCommand(t *testing.T) {
	r := bufio.NewReader(bytes.NewBuffer([]byte{
		0x80,       // Magic
		0xFF,       // Bad opcode
		0x00, 0x00, // key length
		0x00,       // Extra length
		0x00,       // Data type
		0x00, 0x00, // VBucket
		0x00, 0x00, 0x00, 0x04, // total body length
		0x00, 0x00, 0x00, 0xA5, // opaque token
		0x00, 0x00, 0x00, 0x00, // CAS
		0x00, 0x00, 0x00, 0x00, // CAS
	}))
	req, reqType, _, err := NewBinaryParser(r).Parse()

	if req != nil {
		t.Fatal("Expected request struct to be nil")
	}
	if reqType != common.RequestUnknown {
		t.Fatal("Expected request type to be Unknown")
	}
	if err != common.ErrUnknownCmd {
		t.Fatal("Expected error to be Unknown Command")
	}
}

type dummyIO struct{}

func (d dummyIO) Read(p []byte) (int, error) {
	return len(p), nil
}

func (d dummyIO) Write(p []byte) (int, error) {
	return len(p), nil
}

var (
	reqHeaderBenchmarkSink RequestHeader
	resHeaderBenchmarkSink ResponseHeader
	errBenchmarkSink       error
)

func BenchmarkHeaders(b *testing.B) {
	b.Run("readRequestHeader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reqHeaderBenchmarkSink, errBenchmarkSink = readRequestHeader(dummyIO{})
		}
	})
	b.Run("writeRequestHeader", func(b *testing.B) {
		temp := RequestHeader{}
		for i := 0; i < b.N; i++ {
			errBenchmarkSink = writeRequestHeader(dummyIO{}, temp)
		}
	})
	b.Run("ReadResponseHeader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			resHeaderBenchmarkSink, errBenchmarkSink = ReadResponseHeader(dummyIO{})
		}
	})
	b.Run("writeResponseHeader", func(b *testing.B) {
		temp := ResponseHeader{}
		for i := 0; i < b.N; i++ {
			errBenchmarkSink = writeResponseHeader(dummyIO{}, temp)
		}
	})
}
