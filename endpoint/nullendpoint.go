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

package endpoint

import (
	"fmt"
	"github.com/Nuvoloso/extentstore/metadata"
	"github.com/Nuvoloso/extentstore/stats"
)

type nullEndpoint struct {
	purpose Purpose
	stats   *stats.Stats
}

func (nep *nullEndpoint) Config(p Purpose, arg string) error {
	nep.purpose = p
	nep.stats = &stats.Stats{}
	nep.stats.Init()

	fmt.Printf("%s Null Endpoint\n", p.String())

	return nil
}

// A null differ doesn't find any differences
func (nep nullEndpoint) Differ(dataChan chan *metadata.ChunkInfo, shouldAbort AbortCheck) error {
	defer close(dataChan)
	return nil
}

// FetchAt shouldn't be called since the Differ never generates diffs
func (nep nullEndpoint) FetchAt(offset metadata.OffsetT, hint string, buf []byte) error {
	nep.stats.Inc("reads")

	for b := 0; b < metadata.ObjSize; b++ {
		buf[b] = 0
	}

	return nil
}

// FillAt should just ignore the call
func (nep nullEndpoint) FillAt(offset metadata.OffsetT, hint string, buf []byte) error {
	nep.stats.Inc("writes")

	return nil
}

func (nep nullEndpoint) SkipAt(offset metadata.OffsetT, hint string) error {
	// Shouldn't be needed unless the Differ starts sending clean
	nep.stats.Inc("unchanged")
	return nil
}

func (nep nullEndpoint) GetIncrSnapName() string {
	return "nullEndpointIncrementalSnap"
}

func (nep nullEndpoint) GetLunSize() int64 {
	return 0
}

func (nep nullEndpoint) Done(success bool) {
	nep.stats.Report("Null " + nep.purpose.String())
}
