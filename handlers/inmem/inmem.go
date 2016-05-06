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

package inmem

import (
	"sync"
	"time"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
)

type entry struct {
	data    []byte
	instime int64
	exptime int64
	flags   uint32
}

type Handler struct {
	data  map[string]entry
	mutex *sync.RWMutex
}

var singleton = &Handler{
	data:  make(map[string]entry),
	mutex: new(sync.RWMutex),
}

func New() (handlers.Handler, error) {
	// return the same singleton map each time so all connections see the same data
	return singleton, nil
}

func (h *Handler) Set(cmd common.SetRequest) error {
	h.mutex.Lock()

	key := string(cmd.Key)
	instime := time.Now().Unix()
	exptime := instime + int64(cmd.Exptime)
	h.data[key] = entry{
		data:    cmd.Data,
		instime: instime,
		exptime: exptime,
		flags:   cmd.Flags,
	}

	h.mutex.Unlock()
	return nil
}

func (h *Handler) Add(cmd common.SetRequest) error {
	h.mutex.Lock()

	key := string(cmd.Key)

	if _, ok := h.data[key]; ok {
		h.mutex.Unlock()
		return common.ErrKeyExists
	}

	instime := time.Now().Unix()
	exptime := instime + int64(cmd.Exptime)

	h.data[key] = entry{
		data:    cmd.Data,
		instime: instime,
		exptime: exptime,
		flags:   cmd.Flags,
	}

	h.mutex.Unlock()
	return nil
}

func (h *Handler) Replace(cmd common.SetRequest) error {
	h.mutex.Lock()

	key := string(cmd.Key)

	if _, ok := h.data[key]; !ok {
		h.mutex.Unlock()
		return common.ErrKeyExists
	}

	instime := time.Now().Unix()
	exptime := instime + int64(cmd.Exptime)

	h.data[key] = entry{
		data:    cmd.Data,
		instime: instime,
		exptime: exptime,
		flags:   cmd.Flags,
	}

	h.mutex.Unlock()
	return nil
}

func (h *Handler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	dataOut := make(chan common.GetResponse, len(cmd.Keys))
	errorOut := make(chan error)

	h.mutex.RLock()

	for idx, bk := range cmd.Keys {
		key := string(bk)
		e, ok := h.data[key]

		if !ok || e.exptime < time.Now().Unix() {
			dataOut <- common.GetResponse{
				Miss:   true,
				Quiet:  cmd.Quiet[idx],
				Opaque: cmd.Opaques[idx],
				Key:    bk,
			}
			continue
		}

		dataOut <- common.GetResponse{
			Miss:   false,
			Quiet:  cmd.Quiet[idx],
			Opaque: cmd.Opaques[idx],
			Flags:  e.flags,
			Key:    bk,
			Data:   e.data,
		}
	}

	h.mutex.RUnlock()

	close(dataOut)
	close(errorOut)
	return dataOut, errorOut
}

func (h *Handler) GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error) {
	dataOut := make(chan common.GetEResponse, len(cmd.Keys))
	errorOut := make(chan error)

	h.mutex.RLock()

	for idx, bk := range cmd.Keys {
		key := string(bk)

		e, ok := h.data[key]

		if !ok || e.exptime < time.Now().Unix() {
			dataOut <- common.GetEResponse{
				Miss:   true,
				Quiet:  cmd.Quiet[idx],
				Opaque: cmd.Opaques[idx],
				Key:    bk,
			}
			continue
		}

		dataOut <- common.GetEResponse{
			Miss:    false,
			Quiet:   cmd.Quiet[idx],
			Opaque:  cmd.Opaques[idx],
			Exptime: uint32(e.exptime - e.instime),
			Flags:   e.flags,
			Key:     bk,
			Data:    e.data,
		}
	}

	h.mutex.RUnlock()

	close(dataOut)
	close(errorOut)
	return dataOut, errorOut
}

func (h *Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	key := string(cmd.Key)

	h.mutex.Lock()

	e, ok := h.data[key]

	if !ok || e.exptime < time.Now().Unix() {
		h.mutex.Unlock()
		return common.GetResponse{
			Miss:   true,
			Opaque: cmd.Opaque,
			Key:    cmd.Key,
		}, nil
	}

	e.instime = time.Now().Unix()
	e.exptime = e.instime + int64(cmd.Exptime)

	h.data[key] = e

	h.mutex.Unlock()

	return common.GetResponse{
		Miss:   false,
		Opaque: cmd.Opaque,
		Flags:  e.flags,
		Key:    cmd.Key,
		Data:   e.data,
	}, nil
}

func (h *Handler) Delete(cmd common.DeleteRequest) error {
	h.mutex.Lock()
	delete(h.data, string(cmd.Key))
	h.mutex.Unlock()
	return nil
}

func (h *Handler) Touch(cmd common.TouchRequest) error {
	key := string(cmd.Key)

	h.mutex.Lock()

	e, ok := h.data[key]

	if !ok || e.exptime < time.Now().Unix() {
		h.mutex.Unlock()
		return common.ErrKeyNotFound
	}

	e.instime = time.Now().Unix()
	e.exptime = e.instime + int64(cmd.Exptime)

	h.data[key] = e

	h.mutex.Unlock()

	return nil
}

func (h *Handler) Close() error {
	return nil
}
