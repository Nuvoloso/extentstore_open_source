// Copyright 2019 Tad Lebeck
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/Nuvoloso/extentstore/endpoint"
	"github.com/Nuvoloso/extentstore/metadata"
	"os"
	"sync"
)

var numWorkers uint

var errorMux sync.Mutex

var globalError error
var errorSet bool

func setError(err error) {
	// Lock required so that first one in wins
	errorMux.Lock()
	if !errorSet {
		globalError = err
		errorSet = true
	}
	errorMux.Unlock()
}

func init() {
	flag.UintVar(&numWorkers, "threads", 20, "Processing Thread Count")
}

// shouldAbort returns true if an error has occurred that is fatal.
// The lock is not obtained here intentionally since the flag only gets
// set to true (atomic) and letting things continue isn't all that bad.
func shouldAbort() bool {
	return errorSet
}

func worker(id uint,
	work chan *metadata.ChunkInfo,
	wg *sync.WaitGroup,
	src endpoint.EndPoint,
	dst endpoint.EndPoint) {

	defer wg.Done()

	buf := make([]byte, metadata.ObjSize)

	for ci := range work {

		if ci.Dirty {
			//fmt.Println("Worker", id, ":", ci.String())
			e := src.FetchAt(ci.Offset, ci.Hint, buf)
			if e != nil {
				setError(errors.New("FetchAt: " + e.Error()))
			}

			e = dst.FillAt(ci.Offset, ci.Hint, buf)
			if e != nil {
				setError(errors.New("FillAt: " + e.Error()))
			}
		} else {
			e := dst.SkipAt(ci.Offset, ci.Hint)
			if e != nil {
				setError(errors.New("SkipAt: " + e.Error()))
			}
		}
	}
}

func main() {
	flag.Parse()

	if numWorkers == 0 {
		numWorkers = 1
	}

	src, dst, err := endpoint.SetupEndpoints(numWorkers)
	if err != nil {
		fmt.Printf("Error Setting up Endpoints: %s\n", err.Error())
		os.Exit(1)
	}

	workChan := make(chan *metadata.ChunkInfo, numWorkers*2)
	var workerwg sync.WaitGroup

	for i := numWorkers; i != 0; i-- {
		go worker(i, workChan, &workerwg, src, dst)
		workerwg.Add(1)
	}

	diffErr := src.Differ(workChan, shouldAbort)

	workerwg.Wait()
	src.Done(!errorSet && diffErr == nil)
	dst.Done(!errorSet && diffErr == nil)

	var failure bool = false

	if errorSet {
		fmt.Printf("Aborted due to Error: %s\n", globalError.Error())
		failure = true
	}

	if diffErr != nil {
		fmt.Printf("Differ: %s\n", diffErr.Error())
		failure = true
	}

	if failure {
		os.Exit(1)
	}

	os.Exit(0)
}
