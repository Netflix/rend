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

import "testing"
import "bytes"
import "io/ioutil"

func TestReadRequestHeader(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		// Realistic get request header
		r := bytes.NewReader([]byte{
			0x80,       // Magic request byte
			OpcodeGet,  // Get opcode
			0x00, 0x04, // key length, big endian uint16
			0x00,       // Extras length
			0x10,       // Data type, unused, always 0
			0x10, 0x10, // VBucket, unused, always 0
			0x00, 0x00, 0x00, 0x04, // total body length, big endian uint32
			0x00, 0x00, 0x00, 0xA5, // opaque token, big endian uint32
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xBB, // CAS, big endian uint64
		})

		reqHeader, err := readRequestHeader(r)
		if err != nil {
			t.Fatalf("Error reading request header: %v", err)
		}

		// Verify all of the fields
		if reqHeader.Magic != 0x80 {
			t.Fatalf("Expected Magic to be 0x80 but got %v", reqHeader.Magic)
		}
		if reqHeader.Opcode != OpcodeGet {
			t.Fatalf("Expected Opcode to be OpcodeGet but got %v", reqHeader.Opcode)
		}
		if reqHeader.KeyLength != 4 {
			t.Fatalf("Expected KeyLength to be 4 but got %v", reqHeader.KeyLength)
		}
		if reqHeader.ExtraLength != 0 {
			t.Fatalf("Expected ExtraLength to be 0 but got %v", reqHeader.ExtraLength)
		}

		// The logic skips parsing the data type because it's not used internally
		// so even though it's set, it's ignored.
		if reqHeader.DataType != 0 {
			t.Fatalf("Expected DataType to be 0 but got %v", reqHeader.DataType)
		}

		// The logic skips parsing the VBucket because it's not used internally
		// so even though it's set, it's ignored.
		if reqHeader.VBucket != 0 {
			t.Fatalf("Expected VBucket to be 0 but got %v", reqHeader.VBucket)
		}
		if reqHeader.TotalBodyLength != 4 {
			t.Fatalf("Expected TotalBodyLength to be 4 but got %v", reqHeader.TotalBodyLength)
		}
		if reqHeader.OpaqueToken != 0xA5 {
			t.Fatalf("Expected OpaqueToken to be 0xA5 but got %v", reqHeader.OpaqueToken)
		}

		// The logic skips parsing the CAS token because it's not used internally
		// so even though it's set, it's ignored.
		if reqHeader.CASToken != 0x0 {
			t.Fatalf("Expected CASToken to be 0xBB but got %v", reqHeader.CASToken)
		}
	})
	t.Run("InvalidMagic", func(t *testing.T) {
		r := bytes.NewReader([]byte{
			0xFF,       // Magic request byte
			OpcodeGet,  // Get opcode
			0x00, 0x04, // key length, big endian uint16
			0x00,       // Extras length
			0x10,       // Data type, unused, always 0
			0x10, 0x10, // VBucket, unused, always 0
			0x00, 0x00, 0x00, 0x04, // total body length, big endian uint32
			0x00, 0x00, 0x00, 0xA5, // opaque token, big endian uint32
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xBB, // CAS, big endian uint64
		})

		_, err := readRequestHeader(r)
		if err == nil {
			t.Fatal("Should have received ErrBadMagic but got no error")
		}

		if err != ErrBadMagic {
			t.Fatalf("Should have received ErrBadMagic but got %v", err)
		}
	})
}

type FakeReader struct{}

func (r FakeReader) Read(p []byte) (n int, err error) {
	return len(p), nil
}

// package global sink variables to disable dead store elimination in the
// compiler optimization pass
var (
	benchmarkRequestHeaderSink RequestHeader
	benchmarkErrorSink         error
)

func BenchmarkReadRequestHeader(b *testing.B) {
	r := FakeReader{}

	for i := 0; i < b.N; i++ {
		benchmarkRequestHeaderSink, benchmarkErrorSink = readRequestHeader(r)
	}
}

func BenchmarkWriteRequestHeader(b *testing.B) {
	rh := RequestHeader{
		Magic:           0x80,
		Opcode:          OpcodeGet,
		KeyLength:       4,
		TotalBodyLength: 4,
		OpaqueToken:     0xDEADBEEF,
	}

	for i := 0; i < b.N; i++ {
		writeRequestHeader(ioutil.Discard, rh)
	}
}

func BenchmarkWriteResponseHeader(b *testing.B) {
	rh := ResponseHeader{
		Magic:           0x81,
		Opcode:          OpcodeGet,
		Status:          StatusSuccess,
		TotalBodyLength: 4096,
		OpaqueToken:     0xDEADBEEF,
	}

	for i := 0; i < b.N; i++ {
		writeResponseHeader(ioutil.Discard, rh)
	}
}
