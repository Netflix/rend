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

package server_test

import (
	"io"
	"runtime"
	"testing"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/server"
)

type ioCloserSpy struct {
	closed bool
}

func (i *ioCloserSpy) Close() error {
	i.closed = true
	return nil
}

type testRequestParser struct {
	req       common.Request
	reqType   common.RequestType
	startTime uint64
	err       error

	called bool
}

// On first call, returns the values in the testRequestParser
// On second call, always returns io.EOF
func (f *testRequestParser) Parse() (common.Request, common.RequestType, uint64, error) {
	if f.called {
		return nil, 0, 0, io.EOF
	}

	f.called = true
	return f.req, f.reqType, f.startTime, f.err
}

type testOrca struct {
	setRes,
	addRes,
	replaceRes,
	appendRes,
	prependRes,
	deleteRes,
	touchRes,
	getRes,
	geteRes,
	gatRes,
	noopRes,
	quitRes,
	versionRes,
	unknownRes error

	called map[string]interface{}
}

func (t *testOrca) Set(req common.SetRequest) error {
	t.called["Set"] = nil
	return t.setRes
}
func (t *testOrca) Add(req common.SetRequest) error {
	t.called["Add"] = nil
	return t.addRes
}
func (t *testOrca) Replace(req common.SetRequest) error {
	t.called["Replace"] = nil
	return t.replaceRes
}
func (t *testOrca) Append(req common.SetRequest) error {
	t.called["Append"] = nil
	return t.appendRes
}
func (t *testOrca) Prepend(req common.SetRequest) error {
	t.called["Prepend"] = nil
	return t.prependRes
}
func (t *testOrca) Delete(req common.DeleteRequest) error {
	t.called["Delete"] = nil
	return t.deleteRes
}
func (t *testOrca) Touch(req common.TouchRequest) error {
	t.called["Touch"] = nil
	return t.touchRes
}
func (t *testOrca) Get(req common.GetRequest) error {
	t.called["Get"] = nil
	return t.getRes
}
func (t *testOrca) GetE(req common.GetRequest) error {
	t.called["GetE"] = nil
	return t.geteRes
}
func (t *testOrca) Gat(req common.GATRequest) error {
	t.called["Gat"] = nil
	return t.gatRes
}
func (t *testOrca) Noop(req common.NoopRequest) error {
	t.called["Noop"] = nil
	return t.noopRes
}
func (t *testOrca) Quit(req common.QuitRequest) error {
	t.called["Quit"] = nil
	return t.quitRes
}
func (t *testOrca) Version(req common.VersionRequest) error {
	t.called["Version"] = nil
	return t.versionRes
}
func (t *testOrca) Unknown(req common.Request) error {
	t.called["Unknown"] = nil
	return t.unknownRes
}
func (t *testOrca) Error(req common.Request, reqType common.RequestType, err error) {}

type testPanicOrca struct{}

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

func TestDefaultServer(t *testing.T) {
	t.Run("Success", func(t *testing.T) {

		testSuccess := func(t *testing.T, expected string, reqType common.RequestType, req common.Request) {
			// Alec Baldwin approves
			closers := []io.Closer{&ioCloserSpy{}, &ioCloserSpy{}}
			orca := &testOrca{called: make(map[string]interface{})}
			rp := &testRequestParser{
				reqType: reqType,
				req:     req,
			}

			s := server.Default(closers, rp, orca)

			go s.Loop()

			// The loop needs a chance to complete its work
			// when the io.EOF response is returned, the server is expected to close all of its connections
			for {
				closed := true
				for _, c := range closers {
					if !c.(*ioCloserSpy).closed {
						closed = false
					}
				}

				if closed {
					break
				}

				runtime.Gosched()
			}

			// Verify the right method is called
			if _, ok := orca.called[expected]; !ok {
				t.Fatalf("Expected %v orca function to be called", expected)
			}
		}

		t.Run("Set", func(t *testing.T) {
			testSuccess(t, "Set", common.RequestSet, common.SetRequest{
				Key:  []byte("key"),
				Data: []byte("data"),
			})
		})

		t.Run("Add", func(t *testing.T) {
			testSuccess(t, "Add", common.RequestAdd, common.SetRequest{
				Key:  []byte("key"),
				Data: []byte("data"),
			})
		})

		t.Run("Replace", func(t *testing.T) {
			testSuccess(t, "Replace", common.RequestReplace, common.SetRequest{
				Key:  []byte("key"),
				Data: []byte("data"),
			})
		})

		t.Run("Append", func(t *testing.T) {
			testSuccess(t, "Append", common.RequestAppend, common.SetRequest{
				Key:  []byte("key"),
				Data: []byte("data"),
			})
		})

		t.Run("Prepend", func(t *testing.T) {
			testSuccess(t, "Prepend", common.RequestPrepend, common.SetRequest{
				Key:  []byte("key"),
				Data: []byte("data"),
			})
		})

		t.Run("Delete", func(t *testing.T) {
			testSuccess(t, "Delete", common.RequestDelete, common.DeleteRequest{
				Key: []byte("key"),
			})
		})

		t.Run("Touch", func(t *testing.T) {
			testSuccess(t, "Touch", common.RequestTouch, common.TouchRequest{
				Key:     []byte("key"),
				Exptime: 42,
			})
		})

		t.Run("Get", func(t *testing.T) {
			testSuccess(t, "Get", common.RequestGet, common.GetRequest{
				Keys:    [][]byte{[]byte("key")},
				Opaques: []uint32{0},
				Quiet:   []bool{false},
			})
		})

		t.Run("GetE", func(t *testing.T) {
			testSuccess(t, "GetE", common.RequestGetE, common.GetRequest{
				Keys:    [][]byte{[]byte("key")},
				Opaques: []uint32{0},
				Quiet:   []bool{false},
			})
		})

		t.Run("Gat", func(t *testing.T) {
			testSuccess(t, "Gat", common.RequestGat, common.GATRequest{
				Key: []byte("key"),
			})
		})

		t.Run("Noop", func(t *testing.T) {
			testSuccess(t, "Noop", common.RequestNoop, common.NoopRequest{})
		})

		t.Run("Quit", func(t *testing.T) {
			testSuccess(t, "Quit", common.RequestQuit, common.QuitRequest{})
		})

		t.Run("Version", func(t *testing.T) {
			testSuccess(t, "Version", common.RequestVersion, common.VersionRequest{})
		})

		t.Run("Unknown", func(t *testing.T) {
			testSuccess(t, "Unknown", common.RequestUnknown, nil)
		})
	})

	t.Run("Panic", func(t *testing.T) {

		testPanic := func(t *testing.T, reqType common.RequestType, req common.Request) {
			// Alec Baldwin approves
			closers := []io.Closer{&ioCloserSpy{}, &ioCloserSpy{}}
			orca := &testPanicOrca{}
			rp := &testRequestParser{
				reqType: reqType,
				req:     req,
			}

			s := server.Default(closers, rp, orca)

			go s.Loop()

			// The loop needs a chance to complete its work
			// when the io.EOF response is returned, the server is expected to close all of its connections
			for {
				closed := true
				for _, c := range closers {
					if !c.(*ioCloserSpy).closed {
						closed = false
					}
				}

				if closed {
					break
				}

				runtime.Gosched()
			}
		}

		t.Run("Set", func(t *testing.T) { testPanic(t, common.RequestSet, common.SetRequest{}) })
		t.Run("Add", func(t *testing.T) { testPanic(t, common.RequestAdd, common.SetRequest{}) })
		t.Run("Replace", func(t *testing.T) { testPanic(t, common.RequestReplace, common.SetRequest{}) })
		t.Run("Append", func(t *testing.T) { testPanic(t, common.RequestAppend, common.SetRequest{}) })
		t.Run("Prepend", func(t *testing.T) { testPanic(t, common.RequestPrepend, common.SetRequest{}) })
		t.Run("Delete", func(t *testing.T) { testPanic(t, common.RequestDelete, common.DeleteRequest{}) })
		t.Run("Touch", func(t *testing.T) { testPanic(t, common.RequestTouch, common.TouchRequest{}) })
		t.Run("Get", func(t *testing.T) { testPanic(t, common.RequestGet, common.GetRequest{}) })
		t.Run("GetE", func(t *testing.T) { testPanic(t, common.RequestGetE, common.GetRequest{}) })
		t.Run("Gat", func(t *testing.T) { testPanic(t, common.RequestGat, common.GATRequest{}) })
		t.Run("Noop", func(t *testing.T) { testPanic(t, common.RequestNoop, common.NoopRequest{}) })
		t.Run("Quit", func(t *testing.T) { testPanic(t, common.RequestQuit, common.QuitRequest{}) })
		t.Run("Version", func(t *testing.T) { testPanic(t, common.RequestVersion, common.VersionRequest{}) })
		t.Run("Unknown", func(t *testing.T) { testPanic(t, common.RequestUnknown, nil) })
	})
}
