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

package binprot_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/netflix/rend/binprot"
	"github.com/netflix/rend/common"
)

func TestUnknownCommand(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader([]byte{
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
	req, reqType, _, err := binprot.NewBinaryParser(r).Parse()

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
