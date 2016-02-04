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

package common

import (
	"errors"

	"github.com/netflix/rend/metrics"
)

var (
	// Common metrics used across packages
	MetricBytesReadRemote     = metrics.AddCounter("bytes_read_remote")
	MetricBytesReadLocal      = metrics.AddCounter("bytes_read_local")
	MetricBytesReadLocalL1    = metrics.AddCounter("bytes_read_local_l1")
	MetricBytesReadLocalL2    = metrics.AddCounter("bytes_read_local_l2")
	MetricBytesWrittenRemote  = metrics.AddCounter("bytes_written_remote")
	MetricBytesWrittenLocal   = metrics.AddCounter("bytes_written_local")
	MetricBytesWrittenLocalL1 = metrics.AddCounter("bytes_written_local_l1")
	MetricBytesWrittenLocalL2 = metrics.AddCounter("bytes_written_local_l2")

	// Errors used across the application
	ErrBadRequest = errors.New("CLIENT_ERROR bad request")
	ErrBadLength  = errors.New("CLIENT_ERROR length is not a valid integer")
	ErrBadFlags   = errors.New("CLIENT_ERROR flags is not a valid integer")

	ErrNoError        = errors.New("Success")
	ErrKeyNotFound    = errors.New("ERROR Key not found")
	ErrKeyExists      = errors.New("ERROR Key already exists")
	ErrValueTooBig    = errors.New("ERROR Value too big")
	ErrInvalidArgs    = errors.New("ERROR Invalid arguments")
	ErrItemNotStored  = errors.New("ERROR Item not stored (CAS didn't match)")
	ErrBadIncDecValue = errors.New("ERROR Bad increment/decrement value")
	ErrAuth           = errors.New("ERROR Authentication error")
	ErrUnknownCmd     = errors.New("ERROR Unknown command")
	ErrNoMem          = errors.New("ERROR Out of memory")
	ErrNotSupported   = errors.New("ERROR Not supported")
	ErrInternal       = errors.New("ERROR Internal error")
	ErrBusy           = errors.New("ERROR Busy")
	ErrTempFailure    = errors.New("ERROR Temporary error")
)

// Make sure to keep this list in sync with the one above
// It should contain all Err* that could come back from
// memcached itself
func IsAppError(err error) bool {
	return err == ErrKeyNotFound ||
		err == ErrKeyExists ||
		err == ErrValueTooBig ||
		err == ErrInvalidArgs ||
		err == ErrItemNotStored ||
		err == ErrBadIncDecValue ||
		err == ErrAuth ||
		err == ErrUnknownCmd ||
		err == ErrNoMem ||
		err == ErrNotSupported ||
		err == ErrInternal ||
		err == ErrBusy ||
		err == ErrTempFailure
}

// Request types for all protocols
type RequestType int

const (
	RequestUnknown = iota
	RequestGet
	RequestGetQ
	RequestGat
	RequestSet
	RequestDelete
	RequestTouch
)

type RequestParser interface {
	Parse() (interface{}, RequestType, error)
}

type Responder interface {
	Set() error
	Get(response GetResponse) error
	GetMiss(response GetResponse) error
	GetEnd(noopEnd bool) error
	GAT(response GetResponse) error
	GATMiss(response GetResponse) error
	Delete() error
	Touch() error
	Error(err error) error
}

type SetRequest struct {
	Key     []byte
	Data    []byte
	Flags   uint32
	Exptime uint32
	Opaque  uint32
}

// Gets are batch by default
type GetRequest struct {
	Keys    [][]byte
	Opaques []uint32
	Quiet   []bool
	NoopEnd bool
}

type DeleteRequest struct {
	Key    []byte
	Opaque uint32
}

type TouchRequest struct {
	Key     []byte
	Exptime uint32
	Opaque  uint32
}

type GATRequest struct {
	Key     []byte
	Exptime uint32
	Opaque  uint32
}

type GetResponse struct {
	Key    []byte
	Data   []byte
	Opaque uint32
	Flags  uint32
	Miss   bool
	Quiet  bool
}
