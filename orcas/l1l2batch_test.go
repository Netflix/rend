// Copyright 2017 Netflix, Inc.
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

package orcas_test

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol/textprot"
)

func TestL1L2BatchOrca(t *testing.T) {
	t.Run("Set", func(t *testing.T) {
		t.Run("L2SetSuccess", func(t *testing.T) {
			t.Run("L1SetError", func(t *testing.T) {
				t.Run("L1DeleteHit", func(t *testing.T) {
					h1 := &testHandler{
						errors: []error{common.ErrNoMem, nil},
					}
					h2 := &testHandler{
						errors: []error{nil},
					}
					output := &bytes.Buffer{}

					l1l2 := orcas.L1L2Batch(h1, h2, textprot.NewTextResponder(bufio.NewWriter(output)))

					err := l1l2.Set(common.SetRequest{})
					if err != nil {
						t.Fatalf("Error should be nil, got %v", err)
					}

					out := string(output.Bytes())

					t.Logf(out)

					if out != "STORED\r\n" {
						t.Fatalf("Expected response 'STORED\\r\\n' but got '%v'", out)
					}

					h1.verifyEmpty(t)
					h2.verifyEmpty(t)
				})
				t.Run("L1DeleteMiss", func(t *testing.T) {
					h1 := &testHandler{
						errors: []error{common.ErrNoMem, common.ErrKeyNotFound},
					}
					h2 := &testHandler{
						errors: []error{nil},
					}
					output := &bytes.Buffer{}

					l1l2 := orcas.L1L2Batch(h1, h2, textprot.NewTextResponder(bufio.NewWriter(output)))

					err := l1l2.Set(common.SetRequest{})
					if err != nil {
						t.Fatalf("Error should be nil, got %v", err)
					}

					out := string(output.Bytes())

					t.Logf(out)

					if out != "STORED\r\n" {
						t.Fatalf("Expected response 'STORED\\r\\n' but got '%v'", out)
					}

					h1.verifyEmpty(t)
					h2.verifyEmpty(t)
				})
				t.Run("L1DeleteMiss", func(t *testing.T) {
					h1 := &testHandler{
						errors: []error{common.ErrNoMem, io.EOF},
					}
					h2 := &testHandler{
						errors: []error{nil},
					}
					output := &bytes.Buffer{}

					l1l2 := orcas.L1L2Batch(h1, h2, textprot.NewTextResponder(bufio.NewWriter(output)))

					err := l1l2.Set(common.SetRequest{})
					if err != nil {
						t.Fatalf("Error should be nil, got %v", err)
					}

					out := string(output.Bytes())

					t.Logf(out)

					if out != "STORED\r\n" {
						t.Fatalf("Expected response 'STORED\\r\\n' but got '%v'", out)
					}

					h1.verifyEmpty(t)
					h2.verifyEmpty(t)
				})
			})
		})
	})
}
