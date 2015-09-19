/**
 * Functions that talk to the local memcached server
 */
package local

import "bufio"
import "bytes"
import "encoding/binary"
import "io"

import "../binprot"
import "../common"
import "../util"

func getMetadata(localReader *bufio.Reader, localWriter *bufio.Writer, key []byte) ([]byte, common.Metadata, error) {
	metaKey := util.MetaKey(key)

	// Read in the metadata for number of chunks, chunk size, etc.
	getCmd := util.GetCommand(metaKey)
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

	// TODO: Handle ERROR response
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
