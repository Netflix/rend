package orcas

import (
	"log"

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
	log.Println("set", string(req.Key))

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
	log.Println("add", string(req.Key))

	// Add in L2 first, since it has the larger state
	metrics.IncCounter(MetricCmdAddL2)
	err := l.l2.Add(req)

	if err != nil {
		// A key already existing is not an error per se, it's a part of the
		// functionality of the add command to respond with a "not stored"
		if err == common.ErrKeyExists {
			metrics.IncCounter(MetricCmdAddNotStoredL2)
			metrics.IncCounter(MetricCmdAddNotStored)
			err = l.res.Add(req.Opaque, false, req.Quiet)
			return nil
		}

		// otherwise we have a real error on our hands
		metrics.IncCounter(MetricCmdAddErrorsL2)
		metrics.IncCounter(MetricCmdAddErrors)
		return err
	}

	metrics.IncCounter(MetricCmdAddStoredL2)

	// Now on to L1
	metrics.IncCounter(MetricCmdAddL1)
	err = l.l1.Add(req)

	if err != nil {
		// A key already existing is not an error per se, it's a part of the
		// functionality of the add command to respond with a "not stored"
		if err == common.ErrKeyExists {
			metrics.IncCounter(MetricCmdAddNotStoredL1)
			metrics.IncCounter(MetricCmdAddNotStored)
			err = l.res.Add(req.Opaque, false, req.Quiet)
			return nil
		}

		// otherwise we have a real error on our hands
		metrics.IncCounter(MetricCmdAddErrorsL1)
		metrics.IncCounter(MetricCmdAddErrors)
		return err
	}

	metrics.IncCounter(MetricCmdAddStoredL1)
	metrics.IncCounter(MetricCmdAddStored)

	return l.res.Add(req.Opaque, true, req.Quiet)
}

func (l *L1L2Orca) Replace(req common.SetRequest) error {
	metrics.IncCounter(MetricCmdReplace)
	log.Println("replace", string(req.Key))

	// Add in L2 first, since it has the larger state
	metrics.IncCounter(MetricCmdReplaceL2)
	err := l.l2.Replace(req)

	if err != nil {
		// A key already existing is not an error per se, it's a part of the
		// functionality of the replace command to respond with a "not stored"
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(MetricCmdReplaceNotStoredL2)
			metrics.IncCounter(MetricCmdReplaceNotStored)
			err = l.res.Replace(req.Opaque, false, req.Quiet)
			return nil
		}

		// otherwise we have a real error on our hands
		metrics.IncCounter(MetricCmdReplaceErrorsL2)
		metrics.IncCounter(MetricCmdReplaceErrors)
		return err
	}

	metrics.IncCounter(MetricCmdReplaceStoredL2)

	// Now on to L1
	metrics.IncCounter(MetricCmdReplaceL1)
	err = l.l1.Replace(req)

	if err != nil {
		// A key already existing is not an error per se, it's a part of the
		// functionality of the replace command to respond with a "not stored"
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(MetricCmdReplaceNotStoredL1)
			metrics.IncCounter(MetricCmdReplaceNotStored)
			err = l.res.Replace(req.Opaque, false, req.Quiet)
			return nil
		}

		// otherwise we have a real error on our hands
		metrics.IncCounter(MetricCmdReplaceErrorsL1)
		metrics.IncCounter(MetricCmdReplaceErrors)
		return err
	}

	metrics.IncCounter(MetricCmdReplaceStoredL1)
	metrics.IncCounter(MetricCmdReplaceStored)

	return l.res.Replace(req.Opaque, true, req.Quiet)
}

func (l *L1L2Orca) Delete(req common.DeleteRequest) error {
	metrics.IncCounter(MetricCmdDelete)
	//log.Println("delete", string(req.Key))

	metrics.IncCounter(MetricCmdDeleteL1)
	err := l.l1.Delete(req)

	if err == nil {
		metrics.IncCounter(MetricCmdDeleteHits)
		metrics.IncCounter(MetricCmdDeleteHitsL1)

		l.res.Delete(req.Opaque)

	} else if err == common.ErrKeyNotFound {
		metrics.IncCounter(MetricCmdDeleteMissesL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdDeleteMisses)
	} else {
		metrics.IncCounter(MetricCmdDeleteErrorsL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdDeleteErrors)
	}

	// TODO: L2 metrics for deletes, delete hits, delete misses, delete errors

	return err
}

func (l *L1L2Orca) Touch(req common.TouchRequest) error {
	metrics.IncCounter(MetricCmdTouch)
	//log.Println("touch", string(req.Key))

	metrics.IncCounter(MetricCmdTouchL1)
	err := l.l1.Touch(req)

	if err == nil {
		metrics.IncCounter(MetricCmdTouchHitsL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdTouchHits)

		l.res.Touch(req.Opaque)

	} else if err == common.ErrKeyNotFound {
		metrics.IncCounter(MetricCmdTouchMissesL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdTouchMisses)
	} else {
		metrics.IncCounter(MetricCmdTouchMissesL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdTouchMisses)
	}

	// TODO: L2 metrics for touches, touch hits, touch misses, touch errors

	return err
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

	metrics.IncCounter(MetricCmdGatL1)
	res, err := l.l1.GAT(req)

	if err == nil {
		if res.Miss {
			metrics.IncCounter(MetricCmdGatMissesL1)
			// TODO: Account for L2
			metrics.IncCounter(MetricCmdGatMisses)
		} else {
			metrics.IncCounter(MetricCmdGatHits)
			metrics.IncCounter(MetricCmdGatHitsL1)
		}
		l.res.GAT(res)
		// There is no GetEnd call required here since this is only ever
		// done in the binary protocol, where there's no END marker.
		// Calling l.res.GetEnd was a no-op here and is just useless.
		//l.res.GetEnd(0, false)
	} else {
		metrics.IncCounter(MetricCmdGatErrors)
		metrics.IncCounter(MetricCmdGatErrorsL1)
	}

	//TODO: L2 metrics for gats, gat hits, gat misses, gat errors

	return err
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
