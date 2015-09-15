/**
 * Functions that talk to the local memcached server
 */
package local

import "bufio"
import "bytes"
import "encoding/binary"
//import "fmt"
import "io"
import "strings"

import "../common"
import "../util"

func getMetadata(localReader *bufio.Reader, localWriter *bufio.Writer, key string) (string, Metadata, error) {
	metaKey := makeMetaKey(key)

	// Read in the metadata for number of chunks, chunk size, etc.
	getCmd := util.GetCommand(metaKey)
	metaBytes := make([]byte, METADATA_SIZE)
	err := getLocalIntoBuf(localReader, localWriter, getCmd, nil, metaBytes)
	if err != nil {
		return "", Metadata{}, err
	}

	var metaData Metadata
	binary.Read(bytes.NewBuffer(metaBytes), binary.LittleEndian, &metaData)

	return metaKey, metaData, nil
}

func setLocal(localWriter *bufio.Writer, cmd string, token *[16]byte, data []byte) error {
	_, err := localWriter.WriteString(cmd)
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

	// Write data end marker
	_, err = localWriter.WriteString("\r\n")
	if err != nil {
		return err
	}

	localWriter.Flush()
	return nil
}

// TODO: Batch get
func getLocalIntoBuf(localReader *bufio.Reader, localWriter *bufio.Writer, cmd string, tokenBuf []byte, buf []byte) error {
	_, err := localWriter.WriteString(cmd)
	if err != nil {
		return err
	}
    
	localWriter.Flush()

	// Read and discard value header line
	line, err := localReader.ReadString('\n')

	if strings.TrimSpace(line) == "END" {
		return common.MISS
	}

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

	// consume END
	_, err = localReader.ReadBytes('\n')

	return nil
}

func deleteLocal(localReader *bufio.Reader, localWriter *bufio.Writer, key string) error {
	deleteCmd := makeDeleteCommand(key)
    
	_, err := localWriter.WriteString(deleteCmd)
	if err != nil {
		return err
	}
    
	err = localWriter.Flush()
	if err != nil {
		return err
	}

	// TODO: Handle ERROR response
	response, err := localReader.ReadString('\n')
	if err != nil {
		return err
	}
    
	return nil
}

func touchLocal(localReader *bufio.Reader, localWriter *bufio.Writer, key string, exptime string) error {
	touchCmd := makeTouchCommand(key, exptime)
    
	_, err := localWriter.WriteString(touchCmd)
	if err != nil {
		return err
	}
    
	err = localWriter.Flush()
	if err != nil {
		return err
	}

	// TODO: Handle ERROR response
	response, err := localReader.ReadString('\n')
	if err != nil {
		return err
	}

	return nil
}
