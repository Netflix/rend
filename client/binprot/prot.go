package binprot

import "encoding/binary"
import "io"
import "math/rand"
import "time"

import "../common"

const MAX_TTL = 3600
var garbage []byte

func init() {
    rand.Seed(time.Now().UnixNano())
    garbage = make([]byte, 4096)
}

type BinProt struct {}

// get a random expiration
func exp() uint32 {
    return uint32(rand.Intn(MAX_TTL))
}

func consumeResponse(reader io.Reader) error {
    res, err := readRes(reader)
    if err != nil {
        return err
    }

    apperr := statusToError(res.Status)

    // read body in regardless of the error in the header
    lr := io.LimitReader(reader, int64(res.BodyLen))
    for err == nil {
        _, err = lr.Read(garbage);
    }

    if apperr != nil && srsErr(apperr) {
        return apperr
    }
    if err == io.EOF {
        return nil
    }
    return err
}

func (b BinProt) Set(reader io.Reader, writer io.Writer, key, value []byte) error {
    // set packet contains the req header, flags, and expiration
    // flags are irrelevant, and are thus zero.
    // expiration could be important, so hammer with random values from 1 sec up to 1 hour

    // Header
    bodylen := 8 + len(key) + len(value)
    writeReq(writer, common.SET, len(key), 8, bodylen)
    // Extras
    binary.Write(writer, binary.BigEndian, uint32(0))
    binary.Write(writer, binary.BigEndian, exp())
    // Body / data
    writer.Write(key)
    writer.Write(value)

    // consume all of the response and discard
    return consumeResponse(reader)
}

func (b BinProt) Get(reader io.Reader, writer io.Writer, key []byte) error {
    // Header
    writeReq(writer, common.GET, len(key), 0, len(key))
    // Body
    writer.Write(key)

    // consume all of the response and discard
    return consumeResponse(reader)
}

func (b BinProt) Delete(reader io.Reader, writer io.Writer, key []byte) error {
    // Header
    writeReq(writer, common.DELETE, len(key), 0, len(key))
    // Body
    writer.Write(key)

    // consume all of the response and discard
    return consumeResponse(reader)
}

func (b BinProt) Touch(reader io.Reader, writer io.Writer, key []byte) error {
    // Header
    writeReq(writer, common.TOUCH, len(key), 4, len(key)+4)
    // Extras
    binary.Write(writer, binary.BigEndian, exp())
    // Body
    writer.Write(key)

    // consume all of the response and discard
    return consumeResponse(reader)
}
