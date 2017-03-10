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

// Package protocol defines the interfaces that protocols need to implement in order to be usable
// within Rend.
package protocol

import "github.com/netflix/rend/common"

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
