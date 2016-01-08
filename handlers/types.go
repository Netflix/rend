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

package handlers

import "bufio"

import "github.com/netflix/rend/common"

type Handler interface {
	Set(cmd common.SetRequest, src *bufio.Reader) error
	Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error)
	GAT(cmd common.GATRequest) (common.GetResponse, error)
	Delete(cmd common.DeleteRequest) error
	Touch(cmd common.TouchRequest) error
	Close() error
}
