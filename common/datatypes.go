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

// Package common contains all protocol-agnostic types and interfaces, as well as some variables
// used by multiple other packages in the application. Metrics are useful in many places for
// tracking bytes written and read. Errors exist here to be the neutral representation of either
// protocol errors or other application errors that happen all over. Other interfaces and types used
// by multiple protocols are here so any protocol can be used on the front end as well as the back
// with the same intermediate representation.
package common

import (
	"errors"

	"github.com/netflix/rend/metrics"
)

const VersionString = "Rend 0.1"

// Common metrics used across packages
var (
	MetricBytesReadRemote     = metrics.AddCounter("bytes_read_remote", nil)
	MetricBytesReadLocal      = metrics.AddCounter("bytes_read_local", nil)
	MetricBytesReadLocalL1    = metrics.AddCounter("bytes_read_local_l1", nil)
	MetricBytesReadLocalL2    = metrics.AddCounter("bytes_read_local_l2", nil)
	MetricBytesWrittenRemote  = metrics.AddCounter("bytes_written_remote", nil)
	MetricBytesWrittenLocal   = metrics.AddCounter("bytes_written_local", nil)
	MetricBytesWrittenLocalL1 = metrics.AddCounter("bytes_written_local_l1", nil)
	MetricBytesWrittenLocalL2 = metrics.AddCounter("bytes_written_local_l2", nil)

	// Errors used across the application
	ErrBadRequest = errors.New("CLIENT_ERROR bad request")
	ErrBadLength  = errors.New("CLIENT_ERROR length is not a valid integer")
	ErrBadFlags   = errors.New("CLIENT_ERROR flags is not a valid integer")
	ErrBadExptime = errors.New("CLIENT_ERROR exptime is not a valid integer")

	ErrNoError        = errors.New("Success")
	ErrKeyNotFound    = errors.New("ERROR Key not found")
	ErrKeyExists      = errors.New("ERROR Key already exists")
	ErrValueTooBig    = errors.New("ERROR Value too big")
	ErrInvalidArgs    = errors.New("ERROR Invalid arguments")
	ErrItemNotStored  = errors.New("ERROR Item not stored")
	ErrBadIncDecValue = errors.New("ERROR Bad increment/decrement value")
	ErrAuth           = errors.New("ERROR Authentication error")
	ErrUnknownCmd     = errors.New("ERROR Unknown command")
	ErrNoMem          = errors.New("ERROR Out of memory")
	ErrNotSupported   = errors.New("ERROR Not supported")
	ErrInternal       = errors.New("ERROR Internal error")
	ErrBusy           = errors.New("ERROR Busy")
	ErrTempFailure    = errors.New("ERROR Temporary error")
)

// IsAppError differentiates between protocol-defined errors that are relatively benign and other
// fatal errors like an IO error because of some socket problem or network issue.
// Make sure to keep this list in sync with the one above. It should contain all Err* that could
// come back from memcached itself
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

// RequestType is the protocol-agnostic identifier for the command
type RequestType int

const (
	// RequestUnknown means the parser doesn't know what the request represents. Valid protocol
	// parsing but invalid values.
	RequestUnknown RequestType = iota

	// RequestGet represents both a single get and a multi-get, which take differen forms in
	// different protocols. This means it can also be the accumulation of many GETQ commands.
	RequestGet

	// RequestGat is a get-and-touch operation that retrieves the information while updating the TTL
	RequestGat

	// RequestGetE is a custom get which returns the TTL remaining with the data
	RequestGetE

	// RequestSet is to insert a new piece of data unconditionally. What that means is different
	// depending on L1 / L2 handling.
	RequestSet

	// RequestAdd will perform the same operations as set, but only if the key does not exist
	RequestAdd

	// RequestReplace will perform the same operations as set, but only if the key already exists
	RequestReplace

	// RequestAppend appends data to the end of the already existing data for a given key. Does not
	// change the flags or TTL values even if they are given.
	RequestAppend

	// RequestPrepend appends data to the end of the already existing data for a given key. Does not
	// change the flags or TTL values even if they are given.
	RequestPrepend

	// RequestDelete deletes a piece of data from all levels of cache
	RequestDelete

	// RequestTouch updates the TTL for the item specified to a new TTL
	RequestTouch

	// RequestNoop does nothing
	RequestNoop

	// RequestQuit closes the connection
	RequestQuit

	// RequestVersion replies with a string designating the current software version
	RequestVersion
)

// RequestParser represents an interface to parse incoming requests. Each protocol provides its own
// implementation. The return value is an interface{}, but not all hope is lost. The return result
// is guaranteed by implementations to be castable to the type that matches the RequestType returned.
type RequestParser interface {
	Parse() (Request, RequestType, uint64, error)
}

// Responder is the interface for a protocol to respond to different commands. It responds in
// whatever way is appropriate, including doing nothing or panic()-ing for unsupported interactions.
// Unsupported interactions are OK to panic() on because they should never be returned from the
// corresponding RequestParser.
type Responder interface {
	Set(opaque uint32, quiet bool) error
	Add(opaque uint32, quiet bool) error
	Replace(opaque uint32, quiet bool) error
	Append(opaque uint32, quiet bool) error
	Prepend(opaque uint32, quiet bool) error
	Get(response GetResponse) error
	GetEnd(opaque uint32, noopEnd bool) error
	GetE(response GetEResponse) error
	GAT(response GetResponse) error
	Delete(opaque uint32) error
	Touch(opaque uint32) error
	Noop(opaque uint32) error
	Quit(opaque uint32, quiet bool) error
	Version(opaque uint32) error
	Error(opaque uint32, reqType RequestType, err error, quiet bool) error
}

type Request interface {
	GetOpaque() uint32
	IsQuiet() bool
}

// SetRequest corresponds to common.RequestSet. It contains all the information required to fulfill
// a set request.
type SetRequest struct {
	Key     []byte
	Data    []byte
	Flags   uint32
	Exptime uint32
	Opaque  uint32
	Quiet   bool
}

func (r SetRequest) GetOpaque() uint32 {
	return r.Opaque
}

func (r SetRequest) IsQuiet() bool {
	return r.Quiet
}

// GetRequest corresponds to common.RequestGet. It contains all the information required to fulfill
// a get requestGets are batch by default, so single gets and batch gets are both represented by the
// same type.
type GetRequest struct {
	Keys       [][]byte
	Opaques    []uint32
	Quiet      []bool
	NoopOpaque uint32
	NoopEnd    bool
}

func (r GetRequest) GetOpaque() uint32 {
	// TODO: better implementation?
	// It's nonsensical but the best way to react in this case since it's a bad situation already.
	// Typically if this method was needed we're already in a fatal error sitation.
	return 0
}

func (r GetRequest) IsQuiet() bool {
	return false
}

// DeleteRequest corresponds to common.RequestDelete. It contains all the information required to
// fulfill a delete request.
type DeleteRequest struct {
	Key    []byte
	Opaque uint32
	Quiet  bool
}

func (r DeleteRequest) GetOpaque() uint32 {
	return r.Opaque
}

func (r DeleteRequest) IsQuiet() bool {
	return r.Quiet
}

// TouchRequest corresponds to common.RequestTouch. It contains all the information required to
// fulfill a touch request.
type TouchRequest struct {
	Key     []byte
	Exptime uint32
	Opaque  uint32
	Quiet   bool
}

func (r TouchRequest) GetOpaque() uint32 {
	return r.Opaque
}

func (r TouchRequest) IsQuiet() bool {
	return r.Quiet
}

// GATRequest corresponds to common.RequestGat. It contains all the information required to fulfill
// a get-and-touch request.
type GATRequest struct {
	Key     []byte
	Exptime uint32
	Opaque  uint32
	Quiet   bool
}

func (r GATRequest) GetOpaque() uint32 {
	return r.Opaque
}

func (r GATRequest) IsQuiet() bool {
	return r.Quiet
}

// QuitRequest corresponds to common.RequestQuit. It contains all the information required to
// fulfill a quit request.
type QuitRequest struct {
	Opaque uint32
	Quiet  bool
}

func (r QuitRequest) GetOpaque() uint32 {
	return r.Opaque
}

func (r QuitRequest) IsQuiet() bool {
	return r.Quiet
}

// NoopRequest corresponds to common.RequestNoop. It contains all the information required to
// fulfill a version request.
type NoopRequest struct {
	Opaque uint32
}

func (r NoopRequest) GetOpaque() uint32 {
	return r.Opaque
}

func (r NoopRequest) IsQuiet() bool {
	return false
}

// VersionRequest corresponds to common.RequestVersion. It contains all the information required to
// fulfill a version request.
type VersionRequest struct {
	Opaque uint32
}

func (r VersionRequest) GetOpaque() uint32 {
	return r.Opaque
}

func (r VersionRequest) IsQuiet() bool {
	return false
}

// GetResponse is used in both RequestGet and RequestGat handling. Both respond in the same manner
// but with different opcodes. It is binary-protocol specific, but is still a part of the interface
// of responder to make the handling code more protocol-agnostic.
type GetResponse struct {
	Key    []byte
	Data   []byte
	Opaque uint32
	Flags  uint32
	Miss   bool
	Quiet  bool
}

// GetEResponse is used in the GetE protocol extension
type GetEResponse struct {
	Key     []byte
	Data    []byte
	Opaque  uint32
	Flags   uint32
	Exptime uint32
	Miss    bool
	Quiet   bool
}
