package server

import (
	"io"
	"log"
	"time"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/orcas"
)

type DefaultServer struct {
	rp    common.RequestParser
	orca  orcas.Orca
	conns []io.Closer
}

func Default(rp common.RequestParser, o orcas.Orca, conns []io.Closer) Server {
	return &DefaultServer{
		rp:    rp,
		orca:  o,
		conns: conns,
	}
}

func (s *DefaultServer) Loop() {
	defer func() {
		if r := recover(); r != nil {
			if r != io.EOF {
				log.Println("Recovered from runtime panic:", r)
				log.Println("Panic location: ", identifyPanic())
			}
		}
	}()

	for {
		start := time.Now()

		request, reqType, err := s.rp.Parse()
		if err != nil {
			if err == common.ErrBadRequest ||
				err == common.ErrBadLength ||
				err == common.ErrBadFlags ||
				err == common.ErrBadExptime {
				s.orca.Error(nil, common.RequestUnknown, err)
				continue
			} else {
				// Otherwise IO error. Abort!
				abort(s.conns, err)
				return
			}
		}

		metrics.IncCounter(MetricCmdTotal)

		// TODO: handle nil
		switch reqType {
		case common.RequestSet:
			err = s.orca.Set(request.(common.SetRequest))
		case common.RequestAdd:
			err = s.orca.Add(request.(common.SetRequest))
		case common.RequestReplace:
			err = s.orca.Replace(request.(common.SetRequest))
		case common.RequestDelete:
			err = s.orca.Delete(request.(common.DeleteRequest))
		case common.RequestTouch:
			err = s.orca.Touch(request.(common.TouchRequest))
		case common.RequestGet:
			err = s.orca.Get(request.(common.GetRequest))
		case common.RequestGat:
			err = s.orca.Gat(request.(common.GATRequest))
		case common.RequestNoop:
			err = s.orca.Noop(request.(common.NoopRequest))
		case common.RequestQuit:
			s.orca.Quit(request.(common.QuitRequest))
			abort(s.conns, err)
			return
		case common.RequestVersion:
			err = s.orca.Version(request.(common.VersionRequest))
		case common.RequestUnknown:
			err = s.orca.Unknown(request)
		}

		if err != nil {
			if common.IsAppError(err) {
				if err != common.ErrKeyNotFound {
					metrics.IncCounter(MetricErrAppError)
				}
				s.orca.Error(request, reqType, err)
			} else {
				metrics.IncCounter(MetricErrUnrecoverable)
				abort(s.conns, err)
				return
			}
		}

		dur := uint64(time.Since(start))
		switch reqType {
		case common.RequestSet:
			metrics.ObserveHist(HistSet, dur)
		case common.RequestAdd:
			metrics.ObserveHist(HistAdd, dur)
		case common.RequestReplace:
			metrics.ObserveHist(HistReplace, dur)
		case common.RequestDelete:
			metrics.ObserveHist(HistDelete, dur)
		case common.RequestTouch:
			metrics.ObserveHist(HistTouch, dur)
		case common.RequestGet:
			metrics.ObserveHist(HistGet, dur)
		case common.RequestGat:
			metrics.ObserveHist(HistGat, dur)
		}
	}
}
