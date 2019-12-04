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
	"io"
	"os"
	"strings"
)

type fileEndpoint struct {
	purpose      Purpose
	baseSnapName string
	incrSnapName string
	fileName     string
	lun          *os.File
	lunSize      int64
	stats        *stats.Stats
}

func (fep *fileEndpoint) Config(p Purpose, arg string) error {
	var err error

	fep.purpose = p
	fep.stats = &stats.Stats{}
	fep.stats.Init()

	fields := strings.Split(arg, ",")

	if len(fields) != 1 {
		return errors.New("File args <lun>")
	}

	fep.fileName = fields[0]
	fi, e := os.Stat(fep.fileName)
	if e != nil {
		return errors.New(e.Error())
	}
	fep.lunSize = fi.Size()

	switch p {
	case destination:
		fep.lun, err = os.OpenFile(fep.fileName, os.O_WRONLY, 0755)
		if err != nil {
			return err
		}
		fep.stats.Set("writes", 0)
		fep.stats.Set("unchanged", 0)
	case source:
		fep.lun, err = os.Open(fep.fileName)
		if err != nil {
			return err
		}
		fep.stats.Set("diffs", 0)
		fep.stats.Set("reads", 0)
	default:
		return errors.New("Invalid Purpose for File Endpoint")
	}

	fmt.Printf("%s File Endpoint on %s\n", p.String(), fep.fileName)

	return nil
}

// We don't have snapshots on the file so we just say that the whole file is modified.
func (fep fileEndpoint) Differ(dataChan chan *metadata.ChunkInfo, shouldAbort AbortCheck) error {
	defer close(dataChan)

	var off metadata.OffsetT

	for off < metadata.OffsetT(fep.lunSize) {
		if shouldAbort() {
			return errors.New("Stopped due to abort condition")
		}
		//fmt.Printf("Differ: %d dirty\n", off)
		dw := &metadata.ChunkInfo{Offset: off, Length: metadata.ObjSize, Dirty: true}
		fep.stats.Inc("diffs")
		dataChan <- dw
		off += metadata.ObjSize
	}
	return nil
}

func (fep fileEndpoint) FetchAt(offset metadata.OffsetT, hint string, buf []byte) error {
	var totalBytes int
	var bytes int
	var e error

	fep.stats.Inc("reads")

	for totalBytes = 0; totalBytes < metadata.ObjSize; {
		bytes, e = fep.lun.ReadAt(buf[totalBytes:], int64(offset+metadata.OffsetT(totalBytes)))

		if e != nil {
			if e == io.EOF {
				for b := bytes; b < metadata.ObjSize; b++ {
					buf[b] = 0
				}
				bytes = metadata.ObjSize - bytes
			} else {
				return errors.New("ReadAt: " + offset.String() + " " + e.Error())
			}
		}
		totalBytes += bytes
	}
	return nil
}

func (fep fileEndpoint) FillAt(offset metadata.OffsetT, hint string, buf []byte) error {
	var totalBytes int64
	var nbytes int
	var e error

	fep.stats.Inc("writes")

	for totalBytes = 0; totalBytes < metadata.ObjSize; {
		nbytes, e = fep.lun.WriteAt(buf[totalBytes:], int64(offset+metadata.OffsetT(totalBytes)))
		if e != nil {
			return errors.New("WriteAt " + offset.String() + " " + e.Error())
		}
		totalBytes += int64(nbytes)
	}
	return nil
}

func (fep fileEndpoint) SkipAt(offset metadata.OffsetT, hint string) error {
	// Shouldn't be needed unless the Differ starts sending clean
	fep.stats.Inc("unchanged")
	return nil
}

func (fep fileEndpoint) GetIncrSnapName() string {
	return fep.incrSnapName
}

func (fep fileEndpoint) GetLunSize() int64 {
	return fep.lunSize
}

func (fep fileEndpoint) Done(success bool) {
	switch fep.purpose {
	case source:
		// fep.baseReader.Close() or close the file?
		// fep.incrReader.Close() or close the file?
	case destination:
		// Wait for it to finish
	default:
		fmt.Printf("Done on a bad Endpoint.")
	}

	fep.stats.Report("File " + fep.purpose.String())
}
