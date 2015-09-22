/**
 * Functions that talk to the local memcached server
 */
package local

import "bufio"
import "bytes"
import "encoding/binary"
import "io"
import "io/ioutil"

import "../binprot"
import "../common"

func getMetadata(localReader *bufio.Reader, localWriter *bufio.Writer, key []byte) ([]byte, common.Metadata, error) {
	metaKey := metaKey(key)

	// Read in the metadata for number of chunks, chunk size, etc.
	getCmd := binprot.GetCmd(metaKey)
	metaBytes := make([]byte, common.METADATA_SIZE)
    
    _, err := localWriter.Write(getCmd)
    if err != nil {
        return nil, common.Metadata{}, err
    }
    
    localWriter.Flush()
    
    resHeader, err := binprot.ReadResponseHeader(localReader)
    if err != nil {
        return nil, common.Metadata{}, err
    }
    
    err = binprot.DecodeError(resHeader)
    if err != nil {
        return nil, common.Metadata{}, err
    }
    
    _, err = io.ReadFull(localReader, metaBytes)
	if err != nil {
		return nil, common.Metadata{}, err
	}

	var metaData common.Metadata
	binary.Read(bytes.NewBuffer(metaBytes), binary.BigEndian, &metaData)

	return metaKey, metaData, nil
}

// TODO: stream data through instead of buffering the entire value
func setLocal(localWriter *bufio.Writer, cmd []byte, token *[16]byte, data []byte) error {
	_, err := localWriter.Write(cmd)
	if err != nil {
		return err
	}

	// Write a token if there is one
	if token != nil {
		_, err = localWriter.Write(token[:])
		if err != nil {
			return err
		}
	}

	// Write value
	_, err = localWriter.Write(data)
	if err != nil {
		return err
	}

	localWriter.Flush()
	return nil
}

func simpleCmdLocal(localReader *bufio.Reader, localWriter *bufio.Writer, cmd []byte) error {
	_, err := localWriter.Write(cmd)
	if err != nil {
		return err
	}
    
	err = localWriter.Flush()
	if err != nil {
		return err
	}

	resHeader, err := binprot.ReadResponseHeader(localReader)
	if err != nil {
		return err
	}
    
    err = binprot.DecodeError(resHeader)
	if err != nil {
		return err
	}
    
	return nil
}

// TODO: Batch get
func getLocalIntoBuf(localReader *bufio.Reader, localWriter *bufio.Writer, cmd []byte, tokenBuf []byte, buf []byte) error {
	_, err := localWriter.Write(cmd)
	if err != nil {
		return err
	}
    
	localWriter.Flush()

	resHeader, err := binprot.ReadResponseHeader(localReader)
	if err != nil {
		return err
	}
    
    err = binprot.DecodeError(resHeader)
	if err != nil {
		return err
	}
    
	return nil

	// Read in token if requested
	if tokenBuf != nil {
		_, err := io.ReadFull(localReader, tokenBuf)
		if err != nil {
			return err
		}
	}

	// Read in value
    if _, err := io.ReadFull(localReader, buf); err != nil {
		return err
	}

	// consume to end. For all data that are not the last chunk,
	// this will be a no-op. For last chunks, it will swallow the
	// padding at the end.
	_, err = ioutil.ReadAll(localReader)

	return nil
}
