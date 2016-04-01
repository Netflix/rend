package orcas

import (
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/metrics"
)

type L1L2Orca struct {
	l1  handlers.Handler
	l2  handlers.Handler
	res common.Responder
}

func L1L2(l1, l2 handlers.Handler, res common.Responder) Orca {
	return &L1L2Orca{
		l1:  l1,
		l2:  l2,
		res: res,
	}
}

func (l *L1L2Orca) Set(req common.SetRequest) error {
	metrics.IncCounter(MetricCmdSet)
	//log.Println("set", string(req.Key))

	// Try L2 first
	metrics.IncCounter(MetricCmdSetL2)
	err := l.l2.Set(req)

	// If we fail to set in L2, don't set in L1
	if err != nil {
		metrics.IncCounter(MetricCmdSetErrorsL2)
		metrics.IncCounter(MetricCmdSetErrors)
		return err
	}
	metrics.IncCounter(MetricCmdSetSuccessL2)

	// Now set in L1. If L1 fails, we log the error and fail the request.
	// If a user was writing a new piece of information, the error would be OK,
	// since the next GET would be able to put the L2 information back into L1.
	// In the case that the user was overwriting information, a failed set in L1
	// and successful one in L2 would leave us inconsistent. If the response was
	// positive in this situation, it would look like the server successfully
	// processed the request but didn't store the information. Clients will
	// retry failed writes. In this case L2 will get two writes for the same key
	// but this is better because it is more correct overall, though less
	// efficient. Note there are no retries at this level.
	//
	// It should be noted that errors on a straight set are nearly always fatal
	// for the connection. It's likely that if this branch is taken that the
	// connections to everyone will be severed (for this one client connection)
	// and that the client will reconnect to try again.
	metrics.IncCounter(MetricCmdSetL1)
	if err := l.l1.Set(req); err != nil {
		metrics.IncCounter(MetricCmdSetErrorsL1)
		metrics.IncCounter(MetricCmdSetErrors)
		return err
	}

	metrics.IncCounter(MetricCmdSetSuccessL1)
	metrics.IncCounter(MetricCmdSetSuccess)

	return l.res.Set(req.Opaque, req.Quiet)
}

func (l *L1L2Orca) Add(req common.SetRequest) error {
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

	// Now on to L1. For L1 we also do an add operation to protect (partially)
	// against concurrent operations modifying the same key. For concurrent sets
	// that complete between the two stages, this will fail, leaving the cache
	// consistent.
	//
	// There is a possibility in add that concurrent deletes will cause an
	// inconsistent state. A concurrent delete could hit in L2 and miss in
	// L1 between the two add operations, causing the L2 to be deleted and
	// the L1 to have the data.
	metrics.IncCounter(MetricCmdAddL1)
	err = l.l1.Add(req)

	if err != nil {
		// This is kind of a problem. What has happened here is that the L2
		// cache has successfully added the key but L1 did not. In this case
		// we have to fail with a ErrKeyExists/Not Stored because the overall
		// operation failed. If we assume a retry on the client, then it will
		// likely fail again at the L2 step.
		//
		// One possible scenario here is that an add started and completed L2,
		// then a set ran to full completion, overwriting the data in L2 then
		// writing into L1, then the second step here ran and got an error.
		if err == common.ErrKeyExists {
			metrics.IncCounter(MetricCmdAddNotStoredL1)
			metrics.IncCounter(MetricCmdAddNotStored)
			return err
		}

		// otherwise we have a real error on our hands
		metrics.IncCounter(MetricCmdAddErrorsL1)
		metrics.IncCounter(MetricCmdAddErrors)
		return err
	}

	metrics.IncCounter(MetricCmdAddStoredL1)
	metrics.IncCounter(MetricCmdAddStored)

	return l.res.Add(req.Opaque, req.Quiet)
}

func (l *L1L2Orca) Replace(req common.SetRequest) error {
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

	// Now on to L1. For a replace, the L2 succeeding means that the key is
	// successfully replaced in L2, but in the middle here "anything can happen"
	// so we have to think about concurrent operations. In a concurrent set
	// situation, both L2 and L1 might have the same value from the set. In this
	// case an add will fail and cause correct behavior. In a concurrent delete
	// that hits in L2 (for the newly replaced data) and hits in L1 (for the
	// data that was about to be replaced) then an add will cause a consistency
	// problem by setting a key that shouldn't exist in L1 because it's not in
	// L2. set and replace have the opposite problem, since they might overwrite
	// a legitimate set that happened concurrently in the middle of the two
	// operations. There is no one operation that solves these, so:
	//
	// The use of replace here explicitly assumes there is no concurrent set for
	// the same key.
	//
	// The other risk here is a concurrent replace for the same key, which will
	// possibly interleave to produce inconsistency in L2 and L1.
	metrics.IncCounter(MetricCmdReplaceL1)
	err = l.l1.Replace(req)

	if err != nil {
		// In this case, the replace worked fine, and we don't worry about it not
		// being replaced in L1 because it did not exist. In this case, L2 has
		// the data and L1 is empty. This is still correct, and the next get
		// would place the data back into L1. Hence, we do not return the error.
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(MetricCmdReplaceNotStoredL1)
			metrics.IncCounter(MetricCmdReplaceNotStored)
			return l.res.Replace(req.Opaque, req.Quiet)
		}

		// otherwise we have a real error on our hands
		metrics.IncCounter(MetricCmdReplaceErrorsL1)
		metrics.IncCounter(MetricCmdReplaceErrors)
		return err
	}

	metrics.IncCounter(MetricCmdReplaceStoredL1)
	metrics.IncCounter(MetricCmdReplaceStored)

	return l.res.Replace(req.Opaque, req.Quiet)
}

func (l *L1L2Orca) Delete(req common.DeleteRequest) error {
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

func (l *L1L2Orca) Touch(req common.TouchRequest) error {
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

	// In the case of concurrent touches with different values, it's possible
	// that the touches for L1 and L2 interleave and produce an inconsistent
	// state. The L2 could be touched long, then L2 and L1 touched short on
	// another request, then L1 touched long. In this case the data in L1 would
	// outlive L2. This situation is uncommon and is therefore discounted.
	metrics.IncCounter(MetricCmdTouchL1)
	if err := l.l1.Touch(req); err != nil {
		// Touch misses in L1 after a hit in L2 are nto a big deal. The
		// touch operation here explicitly does *not* act as a pre-warm putting
		// data into L1. A miss here after a hit is the same as a hit.
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(MetricCmdTouchMissesL1)
			// Note that we increment the overall hits here (not misses) on
			// purpose because L2 hit.
			metrics.IncCounter(MetricCmdTouchHits)
			return l.res.Touch(req.Opaque)
		}

		metrics.IncCounter(MetricCmdTouchErrorsL1)
		metrics.IncCounter(MetricCmdTouchErrors)
		return err
	}

	metrics.IncCounter(MetricCmdTouchHitsL1)
	metrics.IncCounter(MetricCmdTouchHits)

	return l.res.Touch(req.Opaque)
}

func (l *L1L2Orca) Get(req common.GetRequest) error {
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

	// note to self later: gather misses from L1 into a slice and send as gets to L2 in a batch
	// The L2 handler will be able to send it in a batch to L2, which will internally parallelize

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
					// TODO: Account for L2
					metrics.IncCounter(MetricCmdGetMisses)
				} else {
					metrics.IncCounter(MetricCmdGetHits)
					metrics.IncCounter(MetricCmdGetHitsL1)
				}
				l.res.Get(res)
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

	if err == nil {
		l.res.GetEnd(req.NoopOpaque, req.NoopEnd)
	}

	// TODO: L2 metrics for gets, get hits, get misses, get errors

	return err
}

func (l *L1L2Orca) Gat(req common.GATRequest) error {
	metrics.IncCounter(MetricCmdGat)
	//log.Println("gat", string(req.Key))

	// Try L1 first, since it'll be faster if it succeeds
	metrics.IncCounter(MetricCmdGatL1)
	res, err := l.l1.GAT(req)

	// Errors here are genrally fatal to the connection, as something has gone
	// seriously wrong. Bail out early.
	// I should note that this is different than the other commands, where there
	// are some benevolent "errors" that include KeyNotFound or KeyExists. In
	// both Get and GAT the mini-internal-protocol is different because the Get
	// command uses a channel to send results back and an error channel to signal
	// some kind of fatal problem. The result signals non-fatal "errors"; in this
	// case it's ErrKeyNotFound --> res.Miss is true.
	if err != nil {
		metrics.IncCounter(MetricCmdGatErrorsL1)
		metrics.IncCounter(MetricCmdGatErrors)
		return err
	}

	if res.Miss {
		// If we miss here, we have to GAT L2 to get the data, then put it back
		// into L1 with the new TTL.
		metrics.IncCounter(MetricCmdGatMissesL1)

		metrics.IncCounter(MetricCmdGatL2)
		res, err = l.l2.GAT(req)

		// fatal error
		if err != nil {
			metrics.IncCounter(MetricCmdGatErrorsL2)
			metrics.IncCounter(MetricCmdGatErrors)
			return err
		}

		// A miss on L2 after L1 is a true miss
		if res.Miss {
			metrics.IncCounter(MetricCmdGatMissesL2)
			metrics.IncCounter(MetricCmdGatMisses)
			return l.res.GAT(res)
		}

		// Take the data from the L2 GAT and set into L1 with the new TTL.
		// There's several problems that could arise from interleaving of other
		// operations. Another GAT isn't a problem.
		//
		// Intermediate sets might get clobbered in L1 but remain in L2 if we
		// used Set, but since we use Add we should not overwrite a Set that
		// happens between the L2 GAT hit and subsequent L1 reconciliation.
		//
		// Deletes would be a possible problem since a delete hit in L2 and miss
		// in L1 would interleave to have data in L1 not in L2. This is a risk
		// that is understood and accepted. The typical use cases at Netflix
		// will not use deletes concurrently with GATs.
		setreq := common.SetRequest{
			Key:     req.Key,
			Exptime: req.Exptime,
			Flags:   res.Flags,
			Data:    res.Data,
		}

		metrics.IncCounter(MetricCmdGatAddL1)
		if err := l.l1.Add(setreq); err != nil {
			// we were trampled in the middle of performing the GAT operation
			// In this case, it's fine; no error for the overall op. We still
			// want to track this with a metric, though, and return success.
			if err == common.ErrKeyExists {
				metrics.IncCounter(MetricCmdGatAddNotStoredL1)
			} else {
				metrics.IncCounter(MetricCmdGatAddErrorsL1)
				// Gat errors here and not Add. The metrics for L1/L2 correspond to
				// direct interaction with the two. THe overall metrics correspond
				// to the more abstract orchestrator operation.
				metrics.IncCounter(MetricCmdGatErrors)
				return err
			}
		} else {
			metrics.IncCounter(MetricCmdGatAddStoredL1)
		}

		// the overall operation succeeded
		metrics.IncCounter(MetricCmdGatHits)

	} else {
		metrics.IncCounter(MetricCmdGatHitsL1)

		// Set in L2 to play to the L2's strengths (fast writes) and avoid some
		// pitfalls (slow touches and replaces, which require reads)
		//
		// The first possibility is a Set. A set into L2 would possibly cause a
		// concurrent delete to not take, meaning the delete could say it was
		// successful and then a subsequent get call would show the old data
		// that was just deleted.
		//
		// Another option is to just send a touch, which allows us to send less
		// data but gives the possibility of a touch miss on L2, which will be a
		// problematic situation. If we get a touch miss, then we know we are
		// inconsistent but we don't affect concurrent deletes.
		//
		// A third option is to use Replace, which could be helpful to avoid
		// overriding concurrent deletes. This also might cause problems with
		// othr sets at the same time, as it might overwrite a set that just
		// finished.
		//
		// Many heavy users of EVCache at Netflix use GAT commands to lengthen
		// TTLs of their data in use and to shorten the TTL of data they will
		// not be using which is then async TTL'd out. I am explicitly
		// discounting the concurrent delete situation here and accepting that
		// they might not be exactly correct.
		setreq := common.SetRequest{
			Key:     req.Key,
			Exptime: req.Exptime,
			Flags:   res.Flags,
			Data:    res.Data,
		}

		metrics.IncCounter(MetricCmdGatSetL2)
		err := l.l2.Set(setreq)

		if err != nil {
			// set error? Return it as our error. Yes, the GAT succeeded in L1
			// but if L2 didn't get the set, then likely something is seriously
			// wrong.
			metrics.IncCounter(MetricCmdGatSetErrorsL2)
			metrics.IncCounter(MetricCmdGatErrors)
			return err
		}
		metrics.IncCounter(MetricCmdGatSetSuccessL2)

		// overall operation succeeded
		metrics.IncCounter(MetricCmdGatHits)
	}

	return l.res.GAT(res)
}

func (l *L1L2Orca) Noop(req common.NoopRequest) error {
	metrics.IncCounter(MetricCmdNoop)
	return l.res.Noop(req.Opaque)
}

func (l *L1L2Orca) Quit(req common.QuitRequest) error {
	metrics.IncCounter(MetricCmdQuit)
	return l.res.Quit(req.Opaque, req.Quiet)
}

func (l *L1L2Orca) Version(req common.VersionRequest) error {
	metrics.IncCounter(MetricCmdVersion)
	return l.res.Version(req.Opaque)
}

func (l *L1L2Orca) Unknown(req common.Request) error {
	metrics.IncCounter(MetricCmdUnknown)
	return common.ErrUnknownCmd
}

func (l *L1L2Orca) Error(req common.Request, reqType common.RequestType, err error) {
	l.res.Error(req.Opq(), reqType, err)
}
