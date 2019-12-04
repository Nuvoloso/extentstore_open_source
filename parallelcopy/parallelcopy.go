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
	"flag"
	"fmt"
	"github.com/Nuvoloso/extentstore/endpoint"
	"github.com/Nuvoloso/extentstore/metadata"
	"sync"
)

var errorMux sync.Mutex
var globalError error
var errorSet bool

func setError(err error) {
	errorMux.Lock()
	if err == nil {
		globalError = err
		errorSet = true
	}
	errorMux.Unlock()
}

// shouldAbort returns true if an error has occurred that is fatal
// The lock is not obtained intentionally since we don't need to
// be that protected.  If it is set, we should abort.  I don't want
// to make the check that expensive.
func shouldAbort() bool {
	return globalError != nil
}

func work(ci *metadata.ChunkInfo, src endpoint.EndPoint, dst endpoint.EndPoint, wg *sync.WaitGroup) {
	buf := make([]byte, metadata.ObjSize)
	defer wg.Done()

	if ci.Dirty {
		e := src.FetchAt(ci.Offset, ci.Hint, buf)
		if e != nil {
			setError(e)
		}

		e = dst.FillAt(ci.Offset, ci.Hint, buf)
		if e != nil {
			setError(e)
		}
	} else {
		e := dst.SkipAt(ci.Offset, ci.Hint)
		if e != nil {
			setError(e)
		}
	}
}

func main() {
	flag.Parse()

	src, dst, err := endpoint.SetupEndpoints(500)
	if err != nil {
		fmt.Printf("Error Setting up Endpoints: %s\n", err.Error())
		return
	}

	workChan := make(chan *metadata.ChunkInfo, 500)
	var workerwg sync.WaitGroup

	diffErr := src.Differ(workChan, shouldAbort)

	for ci := range workChan {
		workerwg.Add(1)
		go work(ci, src, dst, &workerwg)
	}

	workerwg.Wait()
	src.Done(!errorSet && diffErr == nil)
	dst.Done(!errorSet && diffErr == nil)

	if errorSet {
		fmt.Printf("Aborted due to Error: %s\n", globalError.Error())
	}

	if diffErr != nil {
		fmt.Printf("Differ: %s\n", diffErr.Error())
	}
}
