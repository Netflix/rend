/**
 * Utility functions that produce repeatable data
 */
package common

import "crypto/rand"
import "fmt"
import "math"

func ReadDataIntoBuf(reader *bufio.Reader, buf []byte) error {
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
