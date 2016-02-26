package binprot

import (
	"io"
	"sync"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
)

var bufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 24, 24)
	},
}

var resHeadPool = &sync.Pool{
	New: func() interface{} {
		return ResponseHeader{}
	},
}

var reqHeadPool = &sync.Pool{
	New: func() interface{} {
		return RequestHeader{}
	},
}

var (
	emptyResHeader = ResponseHeader{}
	emptyReqHeader = RequestHeader{}
)

func ReadRequestHeader(r io.Reader) (RequestHeader, error) {
	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)

	br, err := io.ReadFull(r, buf)
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(br))
	if err != nil {
		return emptyReqHeader, err
	}

	if buf[0] != MagicRequest {
		return emptyReqHeader, ErrBadMagic
	}

	var rh RequestHeader
	rh.Magic = buf[0]
	rh.Opcode = buf[1]
	rh.KeyLength = uint16(buf[2])<<8 | uint16(buf[3])
	rh.ExtraLength = buf[4]
	// ignore DataType, unused
	//rh.DataType = buf[5]
	// ignore VBucket, unused
	//rh.VBucket = uint16(buf[6]) << 8 | uint16(buf[7])
	rh.TotalBodyLength = uint32(buf[8])<<24 | uint32(buf[9])<<16 | uint32(buf[10])<<8 | uint32(buf[11])
	rh.OpaqueToken = uint32(buf[12])<<24 | uint32(buf[13])<<16 | uint32(buf[14])<<8 | uint32(buf[15])
	// ignore CAS, unused in rend
	//rh.CASToken = uint64(buf[16]) << 56 | uint64(buf[17]) << 48 | uint64(buf[18]) << 40 | uint64(buf[19]) << 32 |
	//              uint64(buf[20]) << 24 | uint64(buf[21]) << 16 | uint64(buf[22]) << 8 | uint64(buf[23])

	return rh, nil
}

func writeRequestHeader(w io.Writer, rh RequestHeader) error {
	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)

	buf[0] = rh.Magic
	buf[1] = rh.Opcode
	buf[2] = uint8(rh.KeyLength >> 8)
	buf[3] = uint8(rh.KeyLength)
	buf[4] = rh.ExtraLength
	// DataType and VBucket are unused
	buf[5] = 0
	buf[6] = 0
	buf[7] = 0
	buf[8] = uint8(rh.TotalBodyLength >> 24)
	buf[9] = uint8(rh.TotalBodyLength >> 16)
	buf[10] = uint8(rh.TotalBodyLength >> 8)
	buf[11] = uint8(rh.TotalBodyLength)
	buf[12] = uint8(rh.OpaqueToken >> 24)
	buf[13] = uint8(rh.OpaqueToken >> 16)
	buf[14] = uint8(rh.OpaqueToken >> 8)
	buf[15] = uint8(rh.OpaqueToken)

	// zero CAS region
	for i := 16; i < 24; i++ {
		buf[i] = 0
	}

	n, err := w.Write(buf)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))
	return err
}

func ReadResponseHeader(r io.Reader) (ResponseHeader, error) {
	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)

	br, err := io.ReadFull(r, buf)
	metrics.IncCounterBy(common.MetricBytesReadRemote, uint64(br))
	if err != nil {
		return emptyResHeader, err
	}

	if buf[0] != MagicResponse {
		return emptyResHeader, ErrBadMagic
	}

	var rh ResponseHeader
	rh.Magic = buf[0]
	rh.Opcode = buf[1]
	rh.KeyLength = uint16(buf[2])<<8 | uint16(buf[3])
	rh.ExtraLength = buf[4]
	// ignore DataType, unused
	//rh.DataType = buf[5]
	rh.Status = uint16(buf[6])<<8 | uint16(buf[7])
	rh.TotalBodyLength = uint32(buf[8])<<24 | uint32(buf[9])<<16 | uint32(buf[10])<<8 | uint32(buf[11])
	rh.OpaqueToken = uint32(buf[12])<<24 | uint32(buf[13])<<16 | uint32(buf[14])<<8 | uint32(buf[15])
	// ignore CAS, unused in rend
	//rh.CASToken = uint64(buf[16]) << 56 | uint64(buf[17]) << 48 | uint64(buf[18]) << 40 | uint64(buf[19]) << 32 |
	//                     uint64(buf[20]) << 24 | uint64(buf[21]) << 16 | uint64(buf[22]) << 8 | uint64(buf[23])

	return rh, nil
}

func writeResponseHeader(w io.Writer, rh ResponseHeader) error {
	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)

	buf[0] = rh.Magic
	buf[1] = rh.Opcode
	buf[2] = uint8(rh.KeyLength >> 8)
	buf[3] = uint8(rh.KeyLength)
	buf[4] = rh.ExtraLength
	// DataType is unused
	buf[5] = 0
	buf[6] = uint8(rh.Status >> 8)
	buf[7] = uint8(rh.Status)
	buf[8] = uint8(rh.TotalBodyLength >> 24)
	buf[9] = uint8(rh.TotalBodyLength >> 16)
	buf[10] = uint8(rh.TotalBodyLength >> 8)
	buf[11] = uint8(rh.TotalBodyLength)
	buf[12] = uint8(rh.OpaqueToken >> 24)
	buf[13] = uint8(rh.OpaqueToken >> 16)
	buf[14] = uint8(rh.OpaqueToken >> 8)
	buf[15] = uint8(rh.OpaqueToken)

	// zero CAS region
	for i := 16; i < 24; i++ {
		buf[i] = 0
	}

	n, err := w.Write(buf)
	metrics.IncCounterBy(common.MetricBytesWrittenLocal, uint64(n))
	return err
}
