// Copyright 2016 Netflix, Inc.
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
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/orcas"
)

type testPanicOrca struct{}

func testPanicOrcaConst(l1, l2 handlers.Handler, res common.Responder) orcas.Orca {
	return testPanicOrca{}
}

func (t testPanicOrca) Set(req common.SetRequest) error         { panic("test") }
func (t testPanicOrca) Add(req common.SetRequest) error         { panic("test") }
func (t testPanicOrca) Replace(req common.SetRequest) error     { panic("test") }
func (t testPanicOrca) Append(req common.SetRequest) error      { panic("test") }
func (t testPanicOrca) Prepend(req common.SetRequest) error     { panic("test") }
func (t testPanicOrca) Delete(req common.DeleteRequest) error   { panic("test") }
func (t testPanicOrca) Touch(req common.TouchRequest) error     { panic("test") }
func (t testPanicOrca) Get(req common.GetRequest) error         { panic("test") }
func (t testPanicOrca) GetE(req common.GetRequest) error        { panic("test") }
func (t testPanicOrca) Gat(req common.GATRequest) error         { panic("test") }
func (t testPanicOrca) Noop(req common.NoopRequest) error       { panic("test") }
func (t testPanicOrca) Quit(req common.QuitRequest) error       { panic("test") }
func (t testPanicOrca) Version(req common.VersionRequest) error { panic("test") }
func (t testPanicOrca) Unknown(req common.Request) error        { panic("test") }

func (t testPanicOrca) Error(req common.Request, reqType common.RequestType, err error) {}

func TestPanicUnlocksOrca(t *testing.T) {
	t.Run("Multireader", func(t *testing.T) {
		t.Run("Set", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Set(common.SetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Add", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Add(common.SetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Replace", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Replace(common.SetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Append", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Append(common.SetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Prepend", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Prepend(common.SetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Delete", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Delete(common.DeleteRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Touch", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Touch(common.TouchRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Get", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Get(common.GetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("GetE", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.GetE(common.GetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Gat", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Gat(common.GATRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Noop", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Noop(common.NoopRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Quit", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Quit(common.QuitRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Version", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Version(common.VersionRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Unknown", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, true, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Unknown(nil)
			}

			f()
			// if this times out, the test fails
			f()
		})
	})
	t.Run("Singlereader", func(t *testing.T) {
		t.Run("Set", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Set(common.SetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Add", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Add(common.SetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Replace", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Replace(common.SetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Append", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Append(common.SetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Prepend", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Prepend(common.SetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Delete", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Delete(common.DeleteRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Touch", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Touch(common.TouchRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Get", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Get(common.GetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("GetE", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.GetE(common.GetRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Gat", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Gat(common.GATRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Noop", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Noop(common.NoopRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Quit", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Quit(common.QuitRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Version", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Version(common.VersionRequest{})
			}

			f()
			// if this times out, the test fails
			f()
		})
		t.Run("Unknown", func(t *testing.T) {
			loc, _ := orcas.Locked(testPanicOrcaConst, false, 0)
			lo := loc(nil, nil, nil)

			// make a separate function to be able to recover twice
			f := func() {
				defer func() { recover() }()
				lo.Unknown(nil)
			}

			f()
			// if this times out, the test fails
			f()
		})
	})
}
