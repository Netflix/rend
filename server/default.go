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

func Default(conns []io.Closer, rp common.RequestParser, o orcas.Orca) Server {
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
			metrics.IncCounter(MetricCmdSet)
			err = s.orca.Set(request.(common.SetRequest))
		case common.RequestAdd:
			metrics.IncCounter(MetricCmdAdd)
			err = s.orca.Add(request.(common.SetRequest))
		case common.RequestReplace:
			metrics.IncCounter(MetricCmdReplace)
			err = s.orca.Replace(request.(common.SetRequest))
		case common.RequestAppend:
			metrics.IncCounter(MetricCmdAppend)
			err = s.orca.Append(request.(common.SetRequest))
		case common.RequestPrepend:
			metrics.IncCounter(MetricCmdPrepend)
			err = s.orca.Append(request.(common.SetRequest))
		case common.RequestDelete:
			metrics.IncCounter(MetricCmdDelete)
			err = s.orca.Delete(request.(common.DeleteRequest))
		case common.RequestTouch:
			metrics.IncCounter(MetricCmdTouch)
			err = s.orca.Touch(request.(common.TouchRequest))
		case common.RequestGet:
			metrics.IncCounter(MetricCmdGet)
			err = s.orca.Get(request.(common.GetRequest))
		case common.RequestGetE:
			metrics.IncCounter(MetricCmdGetE)
			err = s.orca.GetE(request.(common.GetRequest))
		case common.RequestGat:
			metrics.IncCounter(MetricCmdGat)
			err = s.orca.Gat(request.(common.GATRequest))
		case common.RequestNoop:
			metrics.IncCounter(MetricCmdNoop)
			err = s.orca.Noop(request.(common.NoopRequest))
		case common.RequestQuit:
			metrics.IncCounter(MetricCmdQuit)
			s.orca.Quit(request.(common.QuitRequest))
			abort(s.conns, err)
			return
		case common.RequestVersion:
			metrics.IncCounter(MetricCmdVersion)
			err = s.orca.Version(request.(common.VersionRequest))
		case common.RequestUnknown:
			metrics.IncCounter(MetricCmdUnknown)
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
