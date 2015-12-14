/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package textprot

import "fmt"
import "io"
import "strings"

import "../common"

const VERBOSE = false

// reads a line without needing to use a bufio.Reader
func rl(r io.Reader) (string, error) {
	retval := make([]byte, 1024)
	b := make([]byte, 1)
	cur := 0

	for b[0] != '\n' {
		n, err := r.Read(b)
		if err != nil {
			return "", err
		}
		if n < 1 {
			continue
		}

		retval[cur] = b[0]
		cur++

		if cur >= cap(retval) {
			newretval := make([]byte, 2*len(retval))
			copy(newretval, retval)
			retval = newretval
		}
	}

	return string(retval[:cur]), nil
}

type TextProt struct{}

func (t TextProt) Set(rw io.ReadWriter, key []byte, value []byte) error {
	strKey := string(key)
	if VERBOSE {
		fmt.Printf("Setting key %s to value of length %v\n", strKey, len(value))
	}

	_, err := fmt.Fprintf(rw, "set %s 0 0 %v\r\n", strKey, len(value))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(rw, "%s\r\n", string(value))
	if err != nil {
		return err
	}

	response, err := rl(rw)
	if err != nil {
		return err
	}

	if VERBOSE {
		fmt.Println(response)
		fmt.Printf("Set key %s\n", strKey)
	}

	return nil
}

func (t TextProt) Get(rw io.ReadWriter, key []byte) error {
	strKey := string(key)
	if VERBOSE {
		fmt.Printf("Getting key %s\n", strKey)
	}

	_, err := fmt.Fprintf(rw, "get %s\r\n", strKey)
	if err != nil {
		return err
	}

	// read the header line
	response, err := rl(rw)
	if err != nil {
		return err
	}
	if VERBOSE {
		fmt.Println(response)
	}

	if strings.TrimSpace(response) == "END" {
		if VERBOSE {
			fmt.Println("Empty response / cache miss")
		}
		return nil
	}

	// then read the value
	response, err = rl(rw)
	if err != nil {
		return err
	}
	if VERBOSE {
		fmt.Println(response)
	}

	// then read the END
	response, err = rl(rw)
	if err != nil {
		return err
	}
	if VERBOSE {
		fmt.Println(response)
		fmt.Printf("Got key %s\n", key)
	}
	return nil
}

func (t TextProt) BatchGet(rw io.ReadWriter, keys [][]byte) error {
	if VERBOSE {
		fmt.Printf("Getting keys %v\n", keys)
	}

	cmd := []byte("get")
	space := byte(' ')
	end := []byte("\r\n")

	for _, key := range keys {
		cmd = append(cmd, space)
		cmd = append(cmd, key...)
	}

	cmd = append(cmd, end...)
	
	_, err := fmt.Fprint(rw, string(cmd))
	if err != nil {
		return err
	}

	for {
		// read the header line
		response, err := rl(rw)
		if err != nil {
			return err
		}
		if VERBOSE {
			fmt.Println(response)
		}

		if strings.TrimSpace(response) == "END" {
			if VERBOSE {
				fmt.Println("End of batch response")
			}
			return nil
		}

		// then read the value
		response, err = rl(rw)
		if err != nil {
			return err
		}
	}
}

func (t TextProt) Delete(rw io.ReadWriter, key []byte) error {
	strKey := string(key)
	if VERBOSE {
		fmt.Printf("Deleting key %s\n", strKey)
	}

	_, err := fmt.Fprintf(rw, "delete %s\r\n", strKey)
	if err != nil {
		return err
	}

	response, err := rl(rw)
	if err != nil {
		return err
	}
	if VERBOSE {
		fmt.Println(response)
		fmt.Printf("Deleted key %s\r\n", strKey)
	}
	return nil
}

func (t TextProt) Touch(rw io.ReadWriter, key []byte) error {
	strKey := string(key)
	if VERBOSE {
		fmt.Printf("Touching key %s\n", strKey)
	}

	_, err := fmt.Fprintf(rw, "touch %s %v\r\n", strKey, common.Exp())
	if err != nil {
		return err
	}

	response, err := rl(rw)
	if err != nil {
		return err
	}
	if VERBOSE {
		fmt.Println(response)
		fmt.Printf("Touched key %s\n", strKey)
	}
	return nil
}
