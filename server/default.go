package server

import (
	"bufio"
	"io"
	"log"
	"time"

	"github.com/netflix/rend/binprot"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/textprot"
)

type DefaultServer struct {
	orca orcas.Orca
}

func NewDefaultServer(o orcas.Orca) Server {
	return &DefaultServer{
		orca: o,
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

	remoteReader := bufio.NewReader(remoteConn)
	remoteWriter := bufio.NewWriter(remoteConn)

	var reqParser common.RequestParser
	var responder common.Responder
	var reqType common.RequestType
	var request common.Request

	// A connection is either binary protocol or text. It cannot switch between the two.
	// This is the way memcached handles protocols, so it can be as strict here.
	binary, err := isBinaryRequest(remoteReader)
	if err != nil {
		// must be an IO error. Abort!
		abort([]io.Closer{remoteConn, l1, l2}, err)
		return
	}

	if binary {
		reqParser = binprot.NewBinaryParser(remoteReader)
		responder = binprot.NewBinaryResponder(remoteWriter)
	} else {
		reqParser = textprot.NewTextParser(remoteReader)
		responder = textprot.NewTextResponder(remoteWriter)
	}

	for {
		start := time.Now()

		request, reqType, err = reqParser.Parse()
		if err != nil {
			if err == common.ErrBadRequest ||
				err == common.ErrBadLength ||
				err == common.ErrBadFlags ||
				err == common.ErrBadExptime {
				responder.Error(0, common.RequestUnknown, err)
				continue
			} else {
				// Otherwise IO error. Abort!
				abort([]io.Closer{remoteConn, l1, l2}, err)
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
			abort([]io.Closer{remoteConn, l1, l2}, err)
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
				responder.Error(request.Opq(), reqType, err)
			} else {
				metrics.IncCounter(MetricErrUnrecoverable)
				abort([]io.Closer{remoteConn, l1, l2}, err)
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
