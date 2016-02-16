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

import "bufio"

type Prot interface {
	// Yes, the abstraction is a little bit leaky, but the code
	// in other places benefits from the consistency.
	Set(rw *bufio.ReadWriter, key []byte, value []byte) error
	Add(rw *bufio.ReadWriter, key []byte, value []byte) error
	Replace(rw *bufio.ReadWriter, key []byte, value []byte) error
	Get(rw *bufio.ReadWriter, key []byte) error
	GAT(rw *bufio.ReadWriter, key []byte) error
	BatchGet(rw *bufio.ReadWriter, keys [][]byte) error
	Delete(rw *bufio.ReadWriter, key []byte) error
	Touch(rw *bufio.ReadWriter, key []byte) error
}

type Op int

const (
	Get = iota
	Bget
	Gat
	Set
	Add
	Replace
	Touch
	Delete
)

var AllOps = []Op{Get, Bget, Gat, Set, Add, Replace, Touch, Delete}

func (o Op) String() string {
	switch o {
	case Set:
		return "Set"
	case Add:
		return "Add"
	case Replace:
		return "Replace"
	case Get:
		return "Get"
	case Gat:
		return "Get and Touch"
	case Bget:
		return "Batch Get"
	case Delete:
		return "Delete"
	case Touch:
		return "Touch"
	default:
		return ""
	}
}

type Task struct {
	Cmd   Op
	Key   []byte
	Value []byte
}
