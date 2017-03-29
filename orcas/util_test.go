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
	"testing"

	"github.com/netflix/rend/common"
)

type testHandler struct {
	errors     []error
	responses  []common.GetResponse
	eresponses []common.GetEResponse
}

func (h *testHandler) verifyEmpty(t *testing.T) {
	if len(h.errors) > 0 {
		t.Fatalf("Expected errors to be empty. Left over: %#v", h.errors)
	}
	if len(h.responses) > 0 {
		t.Fatalf("Expected errors to be empty. Left over: %#v", h.responses)
	}
	if len(h.eresponses) > 0 {
		t.Fatalf("Expected errors to be empty. Left over: %#v", h.eresponses)
	}
}

func (h *testHandler) Set(cmd common.SetRequest) error {
	ret := h.errors[0]
	h.errors = h.errors[1:]
	return ret
}
func (h *testHandler) Add(cmd common.SetRequest) error {
	ret := h.errors[0]
	h.errors = h.errors[1:]
	return ret
}
func (h *testHandler) Replace(cmd common.SetRequest) error {
	ret := h.errors[0]
	h.errors = h.errors[1:]
	return ret
}
func (h *testHandler) Append(cmd common.SetRequest) error {
	ret := h.errors[0]
	h.errors = h.errors[1:]
	return ret
}
func (h *testHandler) Prepend(cmd common.SetRequest) error {
	ret := h.errors[0]
	h.errors = h.errors[1:]
	return ret
}
func (h *testHandler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	if len(h.responses) > 0 {
		res := h.responses[0]
		h.responses = h.responses[1:]
		reschan := make(chan common.GetResponse, 1)
		reschan <- res
		close(reschan)

		errchan := make(chan error)
		close(errchan)

		return reschan, errchan
	}

	reschan := make(chan common.GetResponse)
	close(reschan)

	err := h.errors[0]
	h.errors = h.errors[1:]
	errchan := make(chan error, 1)
	errchan <- err
	close(errchan)

	return reschan, errchan
}
func (h *testHandler) GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error) {
	if len(h.eresponses) > 0 {
		res := h.eresponses[0]
		h.eresponses = h.eresponses[1:]
		reschan := make(chan common.GetEResponse, 1)
		reschan <- res
		close(reschan)

		errchan := make(chan error)
		close(errchan)

		return reschan, errchan
	}

	reschan := make(chan common.GetEResponse)
	close(reschan)

	err := h.errors[0]
	h.errors = h.errors[1:]
	errchan := make(chan error, 1)
	errchan <- err
	close(errchan)

	return reschan, errchan
}
func (h *testHandler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	ret := h.errors[0]
	h.errors = h.errors[1:]
	return common.GetResponse{}, ret
}
func (h *testHandler) Delete(cmd common.DeleteRequest) error {
	ret := h.errors[0]
	h.errors = h.errors[1:]
	return ret
}
func (h *testHandler) Touch(cmd common.TouchRequest) error {
	ret := h.errors[0]
	h.errors = h.errors[1:]
	return ret
}
func (h *testHandler) Close() error {
	ret := h.errors[0]
	h.errors = h.errors[1:]
	return ret
}
