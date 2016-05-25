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

package orcas

import (
	"log"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/metrics"
)

type L1L2BatchOrca struct {
	l1  handlers.Handler
	l2  handlers.Handler
	res common.Responder
}

func L1L2Batch(l1, l2 handlers.Handler, res common.Responder) Orca {
	return &L1L2BatchOrca{
		l1:  l1,
		l2:  l2,
		res: res,
	}
}

func (l *L1L2BatchOrca) Set(req common.SetRequest) error {
	metrics.IncCounter(MetricCmdSet)
	//log.Println("set", string(req.Key))

	// Try L2 first
	metrics.IncCounter(MetricCmdSetL2)
	err := l.l2.Set(req)

	// If we fail to set in L2, don't do anything in L1
	if err != nil {
		metrics.IncCounter(MetricCmdSetErrorsL2)
		metrics.IncCounter(MetricCmdSetErrors)
		return err
	}
	metrics.IncCounter(MetricCmdSetSuccessL2)

	// Replace the entry in L1.
	req.Quiet = false
	metrics.IncCounter(MetricCmdSetReplaceL1)
	if err := l.l1.Replace(req); err != nil {
		if err == common.ErrKeyNotFound {
			// For a replace not stored in L1, there's no problem.
			// There is no hot data to replace
			metrics.IncCounter(MetricCmdSetReplaceNotStoredL1)
		} else {
			metrics.IncCounter(MetricCmdSetReplaceErrorsL1)
			metrics.IncCounter(MetricCmdSetErrors)
			return err
		}
	}

	metrics.IncCounter(MetricCmdSetReplaceStoredL1)
	metrics.IncCounter(MetricCmdSetSuccess)

	return l.res.Set(req.Opaque, req.Quiet)
}

func (l *L1L2BatchOrca) Add(req common.SetRequest) error {
	metrics.IncCounter(MetricCmdAdd)
	//log.Println("add", string(req.Key))

	// Add in L2 first, since it has the larger state
	metrics.IncCounter(MetricCmdAddL2)
	err := l.l2.Add(req)

	if err != nil {
		// A key already existing is not an error per se, it's a part of the
		// functionality of the add command to respond with a "not stored" in
		// the form of a ErrKeyExists. Hence no error metrics.
		if err == common.ErrKeyExists {
			metrics.IncCounter(MetricCmdAddNotStoredL2)
			metrics.IncCounter(MetricCmdAddNotStored)
			return err
		}

		// otherwise we have a real error on our hands
		metrics.IncCounter(MetricCmdAddErrorsL2)
		metrics.IncCounter(MetricCmdAddErrors)
		return err
	}

	metrics.IncCounter(MetricCmdAddStoredL2)

	// Replace the entry in L1.
	req.Quiet = false
	metrics.IncCounter(MetricCmdAddReplaceL1)
	if err := l.l1.Replace(req); err != nil {
		if err == common.ErrKeyNotFound {
			// For a replace not stored in L1, there's no problem.
			// There is no hot data to replace
			metrics.IncCounter(MetricCmdAddReplaceNotStoredL1)
		} else {
			metrics.IncCounter(MetricCmdAddReplaceErrorsL1)
			metrics.IncCounter(MetricCmdAddErrors)
			return err
		}
	}

	metrics.IncCounter(MetricCmdAddReplaceStoredL1)
	metrics.IncCounter(MetricCmdAddStored)

	return l.res.Add(req.Opaque, req.Quiet)
}

func (l *L1L2BatchOrca) Replace(req common.SetRequest) error {
	metrics.IncCounter(MetricCmdReplace)
	//log.Println("replace", string(req.Key))

	// Add in L2 first, since it has the larger state
	metrics.IncCounter(MetricCmdReplaceL2)
	err := l.l2.Replace(req)

	if err != nil {
		// A key already existing is not an error per se, it's a part of the
		// functionality of the replace command to respond with a "not stored"
		// in the form of an ErrKeyNotFound. Hence no error metrics.
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(MetricCmdReplaceNotStoredL2)
			metrics.IncCounter(MetricCmdReplaceNotStored)
			return err
		}

		// otherwise we have a real error on our hands
		metrics.IncCounter(MetricCmdReplaceErrorsL2)
		metrics.IncCounter(MetricCmdReplaceErrors)
		return err
	}

	metrics.IncCounter(MetricCmdReplaceStoredL2)

	// Replace the entry in L1.
	req.Quiet = false
	metrics.IncCounter(MetricCmdReplaceReplaceL1)
	if err := l.l1.Replace(req); err != nil {
		if err == common.ErrKeyNotFound {
			// For a replace not stored in L1, there's no problem.
			// There is no hot data to replace
			metrics.IncCounter(MetricCmdReplaceReplaceNotStoredL1)
		} else {
			metrics.IncCounter(MetricCmdReplaceReplaceErrorsL1)
			metrics.IncCounter(MetricCmdReplaceErrors)
			return err
		}
	}

	metrics.IncCounter(MetricCmdReplaceReplaceStoredL1)
	metrics.IncCounter(MetricCmdReplaceStored)

	return l.res.Replace(req.Opaque, req.Quiet)
}

func (l *L1L2BatchOrca) Delete(req common.DeleteRequest) error {
	metrics.IncCounter(MetricCmdDelete)
	//log.Println("delete", string(req.Key))

	// Try L2 first
	metrics.IncCounter(MetricCmdDeleteL2)
	err := l.l2.Delete(req)

	if err != nil {
		// On a delete miss in L2 don't bother deleting in L1. There might be no
		// key at all, or another request may be deleting the same key. In that
		// case the other will finish up. Returning a key not found will trigger
		// error handling to send back an error response.
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(MetricCmdDeleteMissesL2)
			metrics.IncCounter(MetricCmdDeleteMisses)
			return err
		}

		// If we fail to delete in L2, don't delete in L1. This can leave us in
		// an inconsistent state if the request succeeded in L2 but some
		// communication error caused the problem. In the typical deployment of
		// rend, the L1 and L2 caches are both on the same box with
		// communication happening over a unix domain socket. In this case, the
		// likelihood of this error path happening is very small.
		metrics.IncCounter(MetricCmdDeleteErrorsL2)
		metrics.IncCounter(MetricCmdDeleteErrors)
		return err
	}
	metrics.IncCounter(MetricCmdDeleteHitsL2)

	// Now delete in L1. This means we're temporarily inconsistent, but also
	// eliminated the interleaving where the data is deleted from L1, read from
	// L2, set in L1, then deleted in L2. By deleting from L2 first, if L1 goes
	// missing then no other request can undo part of this request.
	metrics.IncCounter(MetricCmdDeleteL1)
	if err := l.l1.Delete(req); err != nil {
		// Delete misses in L1 are fine. If we get here, that means the delete
		// in L2 hit. This isn't a miss per se since the overall effect is a
		// delete. Concurrent deletes might interleave to produce this, or the
		// data might have TTL'd out. Both cases are still fine.
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(MetricCmdDeleteMissesL1)
			metrics.IncCounter(MetricCmdDeleteHits)
			// disregard the miss, don't return the error
			return l.res.Delete(req.Opaque)
		}
		metrics.IncCounter(MetricCmdDeleteErrorsL1)
		metrics.IncCounter(MetricCmdDeleteErrors)
		return err
	}

	metrics.IncCounter(MetricCmdDeleteHitsL1)
	metrics.IncCounter(MetricCmdDeleteHits)

	return l.res.Delete(req.Opaque)
}

func (l *L1L2BatchOrca) Touch(req common.TouchRequest) error {
	metrics.IncCounter(MetricCmdTouch)
	//log.Println("touch", string(req.Key))

	// Try L2 first
	metrics.IncCounter(MetricCmdTouchL2)
	err := l.l2.Touch(req)

	if err != nil {
		// On a touch miss in L2 don't bother touch in L1. The data should be
		// TTL'd out within a second. This is yet another place where it's
		// possible to be inconsistent, but only for a short time. Any
		// concurrent requests will see the same behavior as this one. If the
		// touch misses here, any other request will see the same view.
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(MetricCmdTouchMissesL2)
			metrics.IncCounter(MetricCmdTouchMisses)
			return err
		}

		// If we fail to touch in L2, don't touch in L1. If the touch succeeded
		// but for some reason the communication failed, then this is still OK
		// since L1 can TTL out while L2 still has the data. On the next get
		// request the data would still be retrievable, albeit more slowly.
		metrics.IncCounter(MetricCmdTouchErrorsL2)
		metrics.IncCounter(MetricCmdTouchErrors)
		return err
	}
	metrics.IncCounter(MetricCmdTouchHitsL2)

	// Try touching in L1 to touch hot data. I'd avoid doing anything in L1 here
	// but it can be a problem if someone decides to touch data down instead of
	// up to let it naturally expire. If I don't touch or delete in L1 then data
	// in L1 might live longer. Touching keeps hot data hot, while delete is
	// more disruptive.
	metrics.IncCounter(MetricCmdTouchTouchL1)
	if err := l.l1.Touch(req); err != nil {
		if err == common.ErrKeyNotFound {
			// For a touch miss in L1, there's no problem.
			metrics.IncCounter(MetricCmdTouchTouchMissesL1)
		} else {
			metrics.IncCounter(MetricCmdTouchTouchErrorsL1)
			metrics.IncCounter(MetricCmdTouchErrors)
			return err
		}
	} else {
		metrics.IncCounter(MetricCmdTouchTouchHitsL1)
	}

	metrics.IncCounter(MetricCmdTouchHits)

	return l.res.Touch(req.Opaque)
}

func (l *L1L2BatchOrca) Get(req common.GetRequest) error {
	metrics.IncCounter(MetricCmdGet)
	metrics.IncCounterBy(MetricCmdGetKeys, uint64(len(req.Keys)))
	//debugString := "get"
	//for _, k := range req.Keys {
	//	debugString += " "
	//	debugString += string(k)
	//}
	//println(debugString)

	metrics.IncCounter(MetricCmdGetL1)
	metrics.IncCounterBy(MetricCmdGetKeysL1, uint64(len(req.Keys)))
	resChan, errChan := l.l1.Get(req)

	var err error
	//var lastres common.GetResponse
	l2keys := make([][]byte, 0)
	l2opaques := make([]uint32, 0)
	l2quiets := make([]bool, 0)

	// Read all the responses back from L1.
	// The contract is that the resChan will have GetResponse's for get hits and misses,
	// and the errChan will have any other errors, such as an out of memory error from
	// memcached. If any receive happens from errChan, there will be no more responses
	// from resChan.
	for {
		select {
		case res, ok := <-resChan:
			if !ok {
				resChan = nil
			} else {
				if res.Miss {
					metrics.IncCounter(MetricCmdGetMissesL1)
					l2keys = append(l2keys, res.Key)
					l2opaques = append(l2opaques, res.Opaque)
					l2quiets = append(l2quiets, res.Quiet)
				} else {
					metrics.IncCounter(MetricCmdGetHits)
					metrics.IncCounter(MetricCmdGetHitsL1)
					l.res.Get(res)
				}
			}

		case getErr, ok := <-errChan:
			if !ok {
				errChan = nil
			} else {
				metrics.IncCounter(MetricCmdGetErrors)
				metrics.IncCounter(MetricCmdGetErrorsL1)
				err = getErr
			}
		}

		if resChan == nil && errChan == nil {
			break
		}
	}

	// Time for the same dance with L2
	req = common.GetRequest{
		Keys:       l2keys,
		NoopEnd:    req.NoopEnd,
		NoopOpaque: req.NoopOpaque,
		Opaques:    l2opaques,
		Quiet:      l2quiets,
	}

	metrics.IncCounter(MetricCmdGetL2)
	metrics.IncCounterBy(MetricCmdGetKeysL2, uint64(len(l2keys)))
	resChan, errChan = l.l2.Get(req)
	for {
		select {
		case res, ok := <-resChan:
			if !ok {
				resChan = nil
			} else {
				if res.Miss {
					metrics.IncCounter(MetricCmdGetMissesL2)
					// Missing L2 means a true miss
					metrics.IncCounter(MetricCmdGetMisses)
				} else {
					metrics.IncCounter(MetricCmdGetHitsL2)

					// For batch, don't set in l1. Typically batch users will read
					// data once and not again, so setting in L1 will not be valuable.
					// As well the data is typically just about to be replaced, making
					// it doubly useless.

					// overall operation is considered a hit
					metrics.IncCounter(MetricCmdGetHits)
				}

				getres := common.GetResponse{
					Key:    res.Key,
					Flags:  res.Flags,
					Data:   res.Data,
					Miss:   res.Miss,
					Opaque: res.Opaque,
					Quiet:  res.Quiet,
				}

				l.res.Get(getres)
			}

		case getErr, ok := <-errChan:
			if !ok {
				errChan = nil
			} else {
				metrics.IncCounter(MetricCmdGetErrors)
				metrics.IncCounter(MetricCmdGetEErrorsL2)
				err = getErr
			}
		}

		if resChan == nil && errChan == nil {
			break
		}
	}

	if err == nil {
		return l.res.GetEnd(req.NoopOpaque, req.NoopEnd)
	}

	return err
}

func (l *L1L2BatchOrca) GetE(req common.GetRequest) error {
	// The L1/L2 batch does not support getE, only L1Only does.
	log.Println("[WARN] Use of GetE in L1L2 Batch orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1L2BatchOrca) Gat(req common.GATRequest) error {
	metrics.IncCounter(MetricCmdGat)
	//log.Println("gat", string(req.Key))

	// Perform L2 for correctness, invalidate in L1 later
	metrics.IncCounter(MetricCmdGatL2)
	res, err := l.l2.GAT(req)

	// Errors here are genrally fatal to the connection, as something has gone
	// seriously wrong. Bail out early.
	// I should note that this is different than the other commands, where there
	// are some benevolent "errors" that include KeyNotFound or KeyExists. In
	// both Get and GAT the mini-internal-protocol is different because the Get
	// command uses a channel to send results back and an error channel to signal
	// some kind of fatal problem. The result signals non-fatal "errors"; in this
	// case it's ErrKeyNotFound --> res.Miss is true.
	if err != nil {
		metrics.IncCounter(MetricCmdGatErrorsL2)
		metrics.IncCounter(MetricCmdGatErrors)
		return err
	}

	if res.Miss {
		// We got a true miss here. L1 is assumed to be missing the data if L2
		// does not have it.
		metrics.IncCounter(MetricCmdGatMissesL2)
		metrics.IncCounter(MetricCmdGatMisses)

	} else {
		metrics.IncCounter(MetricCmdGatHitsL2)

		// Success finding and touching the data in L2, but still need to make
		// sure that the data is *not* in L1 by invalidating it.
		touchreq := common.TouchRequest{
			Key:    req.Key,
			Opaque: req.Opaque,
		}

		// Try touching in L1 to touch hot data. See touch impl for reasoning.
		metrics.IncCounter(MetricCmdGatTouchL1)
		if err := l.l1.Touch(touchreq); err != nil {
			if err == common.ErrKeyNotFound {
				// For a touch miss in L1, there's no problem.
				metrics.IncCounter(MetricCmdGatTouchMissesL1)
			} else {
				metrics.IncCounter(MetricCmdGatTouchErrorsL1)
				metrics.IncCounter(MetricCmdGatErrors)
				return err
			}
		} else {
			metrics.IncCounter(MetricCmdGatTouchHitsL1)
		}

		// overall operation succeeded
		metrics.IncCounter(MetricCmdGatHits)
	}

	return l.res.GAT(res)
}

func (l *L1L2BatchOrca) Noop(req common.NoopRequest) error {
	metrics.IncCounter(MetricCmdNoop)
	return l.res.Noop(req.Opaque)
}

func (l *L1L2BatchOrca) Quit(req common.QuitRequest) error {
	metrics.IncCounter(MetricCmdQuit)
	return l.res.Quit(req.Opaque, req.Quiet)
}

func (l *L1L2BatchOrca) Version(req common.VersionRequest) error {
	metrics.IncCounter(MetricCmdVersion)
	return l.res.Version(req.Opaque)
}

func (l *L1L2BatchOrca) Unknown(req common.Request) error {
	metrics.IncCounter(MetricCmdUnknown)
	return common.ErrUnknownCmd
}

func (l *L1L2BatchOrca) Error(req common.Request, reqType common.RequestType, err error) {
	opaque := uint32(0)
	if req != nil {
		opaque = req.Opq()
	}

	l.res.Error(opaque, reqType, err)
}
