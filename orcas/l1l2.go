package orcas

import (
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/metrics"
)

type L1L2 struct {
	l1  handlers.Handler
	l2  handlers.Handler
	res common.Responder
}

func NewL1L2(deps Deps) *L1L2 {
	return &L1L2{
		l1:  deps.L1,
		l2:  deps.L2,
		res: deps.Res,
	}
}

func (l *L1L2) Set(req common.SetRequest) error {
	metrics.IncCounter(MetricCmdSet)
	//log.Println("set", string(req.Key))

	metrics.IncCounter(MetricCmdSetL1)
	err := l.l1.Set(req)

	if err == nil {
		metrics.IncCounter(MetricCmdSetSuccessL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdSetSuccess)

		err = l.res.Set(req.Opaque, req.Quiet)

	} else {
		metrics.IncCounter(MetricCmdSetErrorsL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdSetErrors)
	}

	// TODO: L2 metrics for sets, set success, set errors

	return err
}

func (l *L1L2) Add(req common.SetRequest) error {
	metrics.IncCounter(MetricCmdAdd)
	//log.Println("add", string(req.Key))

	// TODO: L2 first, then L1

	metrics.IncCounter(MetricCmdAddL1)
	err := l.l1.Add(req)

	if err == nil {
		metrics.IncCounter(MetricCmdAddStoredL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdAddStored)

		err = l.res.Add(req.Opaque, true, req.Quiet)

	} else if err == common.ErrKeyExists {
		metrics.IncCounter(MetricCmdAddNotStoredL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdAddNotStored)
		err = l.res.Add(req.Opaque, false, req.Quiet)
	} else {
		metrics.IncCounter(MetricCmdAddErrorsL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdAddErrors)
	}

	// TODO: L2 metrics for adds, add stored, add not stored, add errors

	return err
}

func (l *L1L2) Replace(req common.SetRequest) error {
	metrics.IncCounter(MetricCmdReplace)
	//log.Println("replace", string(req.Key))

	// TODO: L2 first, then L1

	metrics.IncCounter(MetricCmdReplaceL1)
	err := l.l1.Replace(req)

	if err == nil {
		metrics.IncCounter(MetricCmdReplaceStoredL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdReplaceStored)

		err = l.res.Replace(req.Opaque, true, req.Quiet)

	} else if err == common.ErrKeyNotFound {
		metrics.IncCounter(MetricCmdReplaceNotStoredL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdReplaceNotStored)
		err = l.res.Replace(req.Opaque, false, req.Quiet)
	} else {
		metrics.IncCounter(MetricCmdReplaceErrorsL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdReplaceErrors)
	}

	// TODO: L2 metrics for replaces, replace stored, replace not stored, replace errors

	return err
}

func (l *L1L2) Delete(req common.DeleteRequest) error {
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

func (l *L1L2) Touch(req common.TouchRequest) error {
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

func (l *L1L2) Get(req common.GetRequest) error {
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

func (l *L1L2) Gat(req common.GATRequest) error {
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

func (l *L1L2) Quit(req common.QuitRequest) error {
	metrics.IncCounter(MetricCmdQuit)
	return l.res.Quit(req.Opaque, req.Quiet)
}

func (l *L1L2) Version(req common.VersionRequest) error {
	metrics.IncCounter(MetricCmdVersion)
	return l.res.Version(req.Opaque)
}

func (l *L1L2) Unknown(req common.Request) error {
	metrics.IncCounter(MetricCmdUnknown)
	return common.ErrUnknownCmd
}
