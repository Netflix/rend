/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package common

import "io"

type Op int

const (
	GET = iota
	SET
	TOUCH
	DELETE
)

type Prot interface {
	Set(rw io.ReadWriter, key []byte, value []byte) error
	Get(rw io.ReadWriter, key []byte) error
	Delete(rw io.ReadWriter, key []byte) error
	Touch(rw io.ReadWriter, key []byte) error
}

type Task struct {
	Cmd   string
	Key   []byte
	Value []byte
}
