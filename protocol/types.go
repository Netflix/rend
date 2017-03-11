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

// Package protocol defines the interfaces that protocols need to implement in order to be usable
// within Rend.
package protocol

import (
	"bufio"

	"github.com/netflix/rend/common"
)

// RequestParser represents an interface to parse incoming requests. Each protocol provides its own
// implementation. The return value is a generic Request, but not all hope is lost. The return result
// is guaranteed by implementations to be castable to the type that matches the RequestType returned.
// The return values are the Request value, the type of that value, the uint64 timestamp at which a
// request arrived (for latency timing) and any error that may have occurred.
type RequestParser interface {
	Parse() (common.Request, common.RequestType, uint64, error)
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
	Get(response common.GetResponse) error
	GetEnd(opaque uint32, noopEnd bool) error
	GetE(response common.GetEResponse) error
	GAT(response common.GetResponse) error
	Delete(opaque uint32) error
	Touch(opaque uint32) error
	Noop(opaque uint32) error
	Quit(opaque uint32, quiet bool) error
	Version(opaque uint32) error
	Error(opaque uint32, reqType common.RequestType, err error, quiet bool) error
}

// Peeker is an interface that is designed to restrict the functionality of a bufio.Reader for the
// purposes of protocol disambiguation. A bufio.Reader must be cast to a protocol.Peeker to be used
// in
type Peeker interface {
	Peek(n int) ([]byte, error)
}

// Disambiguator is an interface for protocols to implement to tell the server listener if the protocol
// that implements it is capable of parsing the connection. The protocol will only be able to Peek as
// far as the underlying bufio.Reader can. The assumption here is that the implementation is actually
// a bufio.Reader but implementations should not rely on that fact and only use the Prrk methods.
type Disambiguator interface {
	CanParse() (bool, error)
}

// Components is a package of all the protocol-specific bits of the protocol impelmentation.
// Packages are expected to provide an implementation of this interface to be usable in the
// server.ListenAndServe loop.
type Components interface {
	NewDisambiguator(p Peeker) Disambiguator
	NewRequestParser(r *bufio.Reader) RequestParser
	NewResponder(w *bufio.Writer) Responder
}
