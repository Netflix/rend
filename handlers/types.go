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

import "github.com/netflix/rend/common"

type HandlerConst func() (Handler, error)

// NilHandler is used as a placeholder for when there is no handler needed.
// Since the Server API is a composition of a few things, including Handlers,
// there needs to be a placeholder for when it's not needed.
func NilHandler() (Handler, error) { return nil, nil }

type Handler interface {
	Set(cmd common.SetRequest) error
	Add(cmd common.SetRequest) error
	Replace(cmd common.SetRequest) error
	Append(cmd common.SetRequest) error
	Prepend(cmd common.SetRequest) error
	Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error)
	GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error)
	GAT(cmd common.GATRequest) (common.GetResponse, error)
	Delete(cmd common.DeleteRequest) error
	Touch(cmd common.TouchRequest) error
	Close() error
}
