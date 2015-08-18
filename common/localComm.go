/**
 * Functions that talk to the local memcached server
 */
package common

import "bufio"
import "bytes"
import "encoding/binary"
import "fmt"
import "io"
import "strings"

func readDataIntoBuf(reader *bufio.Reader, buf []byte) error {
	if verbose {
		fmt.Println("readDataIntoBuf")
		fmt.Println("buf length:", len(buf))
	}

	// TODO: real error handling for not enough bytes
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return err
	}

	// Consume the <buffer bytes>\r\n at the end of the data
	endData, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}

	if verbose {
		fmt.Println("End data:", endData)
	}

	return nil
}

func setLocal(localWriter *bufio.Writer, cmd string, token *[16]byte, data []byte) error {
	// Write key/cmd
	if verbose {
		fmt.Println("cmd:", cmd)
	}
    
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
	// Write key/cmd
	if verbose {
		fmt.Println("cmd:", cmd)
	}
    
	_, err := localWriter.WriteString(cmd)
	if err != nil {
		return err
	}
    
	localWriter.Flush()

	// Read and discard value header line
	line, err := localReader.ReadString('\n')

	if strings.TrimSpace(line) == "END" {
		if verbose {
			fmt.Println("Cache miss for cmd", cmd)
		}
		return MISS
	}

	if verbose {
		fmt.Println("Header:", string(line))
	}

	// Read in token if requested
	if tokenBuf != nil {
		_, err := io.ReadFull(localReader, tokenBuf)
		if err != nil {
			return err
		}
	}

	// Read in value
	err = readDataIntoBuf(localReader, buf)
	if err != nil {
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
    
	if verbose {
		fmt.Println("Delete response:", response)
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
    
	if verbose {
		fmt.Println("Touch response:", response)
	}

	return nil
}

func getMetadata(localReader *bufio.Reader, localWriter *bufio.Writer, key string) (string, Metadata, error) {
	metaKey := makeMetaKey(key)

	if verbose {
		fmt.Println("metaKey:", metaKey)
	}

	// Read in the metadata for number of chunks, chunk size, etc.
	getCmd := makeGetCommand(metaKey)
	metaBytes := make([]byte, METADATA_SIZE)
	err := getLocalIntoBuf(localReader, localWriter, getCmd, nil, metaBytes)
	if err != nil {
		return "", Metadata{}, err
	}

	if verbose {
		fmt.Printf("% x\r\n", metaBytes)
	}

	var metaData Metadata
	binary.Read(bytes.NewBuffer(metaBytes), binary.LittleEndian, &metaData)

	if verbose {
		fmt.Println("Metadata:", metaData)
	}

	return metaKey, metaData, nil
}
