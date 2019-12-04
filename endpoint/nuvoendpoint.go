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
	"errors"
	"fmt"
	"github.com/Nuvoloso/extentstore/metadata"
	"github.com/Nuvoloso/extentstore/stats"
	"github.com/Nuvoloso/kontroller/pkg/nuvoapi"
	"os"
	"strconv"
	"strings"
	"sync"
)

type nuvoEndpoint struct {
	purpose      Purpose
	volUUID      string
	baseSnapName string
	incrSnapName string
	mdwg         *sync.WaitGroup
	lunSize      metadata.OffsetT
	stats        *stats.Stats
	NuvoAPI      nuvoapi.NuvoVM
}

func (nep *nuvoEndpoint) Config(p Purpose, arg string) error {
	nep.purpose = p
	nep.stats = &stats.Stats{}
	nep.stats.Init()

	fields := strings.Split(arg, ",")
	if len(fields) != 5 {
		return errors.New(p.String() + " Nuvo Incorrect number of arguments")
	}
	nep.volUUID = fields[0]
	nep.baseSnapName = fields[1]
	nep.incrSnapName = fields[2]

	// Should have a volume size for each snapshot
	val, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil {
		return errors.New(p.String() + " Nuvo Invalid volume size")
	}

	nep.lunSize = metadata.OffsetT(val)

	nep.NuvoAPI = nuvoapi.NewNuvoVM(fields[4])
	_, ok := nep.NuvoAPI.(*nuvoapi.DoSendRecver)
	if !ok {
		return errors.New(p.String() + " Nuvo Cannot contact VM")
	}

	switch p {
	case destination:
		nep.mdwg = &sync.WaitGroup{}
		nep.mdwg.Add(1)

		nep.stats.Set("uploads", 0)
		nep.stats.Set("writes", 0)
		nep.stats.Set("dedups", 0)
		nep.stats.Set("unchanged", 0)
	case source:
		nep.lunSize = 500 * 1024 * 1024 // this needs to be set to the correct value
		nep.stats.Set("clean", 0)
		nep.stats.Set("dirty", 0)
		nep.stats.Set("reads", 0)
	default:
		return errors.New("Invalid Purpose for Dir Endpoint")
	}

	fmt.Printf("%s Nuvo Endpoint Vol %s, size %dM\n", p.String(), nep.volUUID, nep.lunSize)

	return nil
}

// Get a bunch of diffs from storelandia
func getNextDiffs(nep *nuvoEndpoint, off metadata.OffsetT) []*metadata.ChunkInfo {
	var diffBlock [1]*metadata.ChunkInfo

	// Currently this routine just builds one entry
	// that says the whole volume is dirty.

	// I expect when storelandia has the capability
	// to generate diffs, it will generate diffs of
	// different sizes based on the level of table
	// it finds to be different.  It is expected that
	// all offsets and lengths will be multiples of
	// 1M and we will get clean or dirty for the whole
	// range of the volume.

	if off >= nep.lunSize {
		return diffBlock[0:0]
	}

	//    nuvoAPI := nuvoapi.NewNuvoVM(nep.socket)
	//    err := nuvoAPI.GetPitDiffs(nep.)

	diff := &metadata.ChunkInfo{
		Offset: off,
		Length: nep.lunSize,
		Dirty:  true}

	diffBlock[0] = diff

	return diffBlock[:]
}

func (nep nuvoEndpoint) Differ(dataChan chan *metadata.ChunkInfo, shouldAbort AbortCheck) error {
	defer close(dataChan)

	// while we haven't hit the end of the volume
	for off := metadata.OffsetT(0); off < nep.lunSize; {
		offdiffs := getNextDiffs(&nep, off)

		// while we have some diffs to process
		for i := 0; i < len(offdiffs); i++ {
			diff := offdiffs[i]

			// We need this if the differ will skip
			// unchanged ranges without a diff record
			if diff.Offset != off {
				nep.stats.Inc("madecleandiff")
				// Create an entry that says not dirty
				// for the whole gap.  The next loop
				// will break it down to sizeable chunks.
				diff = &metadata.ChunkInfo{
					Offset: off,
					Length: diff.Offset - off,
					Dirty:  false,
				}
				i-- // Do this diff next time
			}

			if diff.Length%metadata.ObjSize != 0 {
				return errors.New("Misaligned Diff from Storelandia")
			}

			// Produce all properly sized diffs to be processed
			for consumed := metadata.OffsetT(0); consumed < diff.Length; consumed += metadata.ObjSize {

				if shouldAbort() {
					return errors.New("Stopped due to abort condition")
				}

				if diff.Dirty {
					nep.stats.Inc("dirty")
				} else {
					nep.stats.Inc("clean")
				}
				dataChan <- &metadata.ChunkInfo{
					Offset: diff.Offset + consumed,
					Length: metadata.ObjSize,
					Dirty:  diff.Dirty,
				}
			}
			off += diff.Length
		}
	}
	return nil
}

func (nep nuvoEndpoint) FetchAt(offset metadata.OffsetT, hint string, buf []byte) error {
	var totalBytes int64
	var bytes int

	nep.stats.Inc("reads")

	// read in the object
	for totalBytes = 0; totalBytes < metadata.ObjSize; {
		buf[0] = 0
		bytes = 1
		totalBytes += int64(bytes)
	}

	return nil
}

func (nep nuvoEndpoint) FillAt(offset metadata.OffsetT, hint string, buf []byte) error {
	var totalBytes int
	var nbytes int
	var obj *os.File
	var e error

	nep.stats.Inc("writes")

	for totalBytes = 0; totalBytes < metadata.ObjSize; {
		nbytes, e = obj.Write(buf[totalBytes:metadata.ObjSize])
		if e != nil {
			return errors.New("Write: " + e.Error())
		}
		totalBytes += nbytes
	}

	return nil
}

func (nep nuvoEndpoint) SkipAt(offset metadata.OffsetT, hint string) error {
	nep.stats.Inc("unchanged")
	return nil
}

func (nep nuvoEndpoint) GetIncrSnapName() string {
	return nep.incrSnapName
}

func (nep nuvoEndpoint) GetLunSize() int64 {
	// This should return a uint64 instead
	return int64(nep.lunSize)
}

func (nep nuvoEndpoint) Done(success bool) {
	switch nep.purpose {
	case source:
		// nep.baseReader.Close() or close the file?
		// nep.incrReader.Close() or close the file?
	case destination:
		// Close the mdChan to stop the metadata handler
		// Wait for it to finish
		nep.mdwg.Wait()
	default:
		fmt.Printf("Done on a bad Endpoint.")
	}
	nep.stats.Report("Nuvo " + nep.purpose.String())
}
