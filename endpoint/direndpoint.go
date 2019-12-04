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
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/Nuvoloso/extentstore/encrypt"
	"github.com/Nuvoloso/extentstore/metadata"
	"github.com/Nuvoloso/extentstore/stats"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type dirEndpoint struct {
	purpose       Purpose
	baseSnapName  string
	incrSnapName  string
	baseDirectory string
	esName        string
	volName       string
	baseReader    *csv.Reader
	incrReader    *csv.Reader
	mdChan        chan *metadata.Entry
	mdwg          *sync.WaitGroup
	lunSize       int64
	stats         *stats.Stats
	key           *[]byte
}

// This is a debug option to allow the quick turn on/off of encryption
// All this does is set the key to nil so that the rest of the code
// will not use encryption.
var dirUseEncryption bool = true

func (dep *dirEndpoint) Config(p Purpose, arg string, workerCount uint) error {
	// break up the argument string
	// set the state
	var err error

	dep.purpose = p
	dep.stats = &stats.Stats{}
	dep.stats.Init()

	fields := strings.Split(arg, ",")
	if len(fields) != 5 {
		return errors.New(p.String() + "directory arguments should be <dir>,<es name>,<volume>,<basesnap>,<incrsnap>")
	}
	dep.baseDirectory = fields[0]
	dep.esName = fields[1]
	dep.volName = fields[2]
	dep.baseSnapName = fields[3]
	dep.incrSnapName = fields[4]

	switch p {
	case destination:
		// make sure the directory structure for data is present
		e := createDirs(dep.baseDirectory + "/" + metadata.HashToObjID("X"))
		if e != nil {
			return errors.New("Failed base directory create " + err.Error())
		}
		if dep.baseSnapName != "" {
			baseSnapObjectName := metadata.SnapMetadataName(dep.esName,
				dep.volName,
				dep.baseSnapName)
			_, e = os.Stat(dep.baseDirectory + "/" + baseSnapObjectName)
			if os.IsNotExist(e) {
				return errors.New("Base Snapshot Not Present: " + dep.baseSnapName)
			}
		}

		dep.mdwg = &sync.WaitGroup{}
		dep.mdwg.Add(1)
		dep.mdChan = make(chan *metadata.Entry, workerCount*2)
		go dirMetadataHandler(dep)
		dep.stats.Set("uploads", 0)
		dep.stats.Set("writes", 0)
		dep.stats.Set("dedups", 0)
		dep.stats.Set("unchanged", 0)
	case source:
		if dep.baseSnapName == "" {
			dep.baseReader, err = metadata.NewMetaReader("/dev/null")
			if err != nil {
				return errors.New("Can't setup null snapshot reader " + err.Error())
			}
		} else {
			baseSnapObjectName := metadata.SnapMetadataName(dep.esName,
				dep.volName,
				dep.baseSnapName)
			dep.baseReader, err = metadata.NewMetaReader(dep.baseDirectory + "/" + baseSnapObjectName)
			if err != nil {
				return errors.New("Can't setup base snapshot reader " + err.Error())
			}
		}
		incrSnapObjectName := metadata.SnapMetadataName(dep.esName,
			dep.volName,
			dep.incrSnapName)
		dep.incrReader, err = metadata.NewMetaReader(dep.baseDirectory + "/" + incrSnapObjectName)
		if err != nil {
			return errors.New("Can't setup incremental snapshot reader " + err.Error())
		}
		dep.stats.Set("clean", 0)
		dep.stats.Set("dirty", 0)
		dep.stats.Set("reads", 0)
	default:
		return errors.New("Invalid Purpose for Dir Endpoint")
	}

	if dirUseEncryption {
		key := []byte{
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
		}
		dep.key = &key
	}

	var baseName string
	if dep.baseSnapName == "" {
		baseName = "null"
	} else {
		baseName = dep.baseSnapName
	}
	fmt.Printf("%s Directory Endpoint on %s: %s %s, %s -> %s\n", p.String(),
		dep.baseDirectory, dep.esName, dep.volName, baseName, dep.incrSnapName)

	return nil
}

func (dep dirEndpoint) Differ(dataChan chan *metadata.ChunkInfo, shouldAbort AbortCheck) error {
	defer close(dataChan)

	// should make the meta chan internal
	baseMDE := metadata.NextEntry(dep.baseReader, dep.key)
	incrMDE := metadata.NextEntry(dep.incrReader, dep.key)

	for incrMDE.Offset() != metadata.Infinity {
		if shouldAbort() {
			return errors.New("Stopped due to abort condition")
		}
		if incrMDE.Offset() == baseMDE.Offset() {
			if incrMDE.ObjID == baseMDE.ObjID {
				dep.stats.Inc("clean")
				// No Difference
				dataChan <- &metadata.ChunkInfo{
					Offset: incrMDE.Offset(),
					Length: metadata.ObjSize,
					Hint:   incrMDE.ObjID,
					Dirty:  false,
				}
			} else {
				dep.stats.Inc("dirty")
				// Difference
				dataChan <- &metadata.ChunkInfo{
					Offset: incrMDE.Offset(),
					Length: metadata.ObjSize,
					Hint:   incrMDE.ObjID,
					Dirty:  true,
				}
			}
			baseMDE = metadata.NextEntry(dep.baseReader, dep.key)
			incrMDE = metadata.NextEntry(dep.incrReader, dep.key)
		} else if incrMDE.Offset() < baseMDE.Offset() {
			dep.stats.Inc("dirty")
			// Difference
			dataChan <- &metadata.ChunkInfo{
				Offset: incrMDE.Offset(),
				Length: metadata.ObjSize,
				Hint:   incrMDE.ObjID,
				Dirty:  true,
			}
			incrMDE = metadata.NextEntry(dep.incrReader, dep.key)
		} else if incrMDE.Offset() > baseMDE.Offset() {
			// How can this happen?
			// Since fixed Span Size not a diff
			baseMDE = metadata.NextEntry(dep.baseReader, dep.key)
		}
	}

	return nil
}

func (dep dirEndpoint) FetchAt(offset metadata.OffsetT, hint string, buf []byte) error {
	// hit is the ObjID just fetch that.
	// Otherwise we would need to look through the metadata to find the object.

	if hint == "" {
		return errors.New("Hint Not Set")
	}

	dep.stats.Inc("reads")
	// hint is the hash
	// Do we trust that?
	o, e := os.Open(dep.baseDirectory + "/" + metadata.HashToObjID(hint))
	if e != nil {
		return errors.New(e.Error())
	}
	defer o.Close()

	if dep.key == nil {
		// read in the object
		r := io.LimitReader(o, metadata.ObjSize+1024)
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return errors.New(e.Error())
		}
		if n != metadata.ObjSize {
			return errors.New("Object Size Off")
		}
	} else {
		ebuf := make([]byte, metadata.ObjSize+1024)

		r := io.LimitReader(o, metadata.ObjSize+1024)
		n, err := r.Read(ebuf)

		if err != nil && err != io.EOF {
			return errors.New("File Read: " + e.Error())
		}

		if n == metadata.ObjSize+1024 { // this means we ran out of buffer space
			return errors.New("Encryption buffer is the wrong size")
		}

		dataBuf, err := encrypt.Decrypt(ebuf[:n], *dep.key)
		if err != nil {
			return errors.New("Decrypt: " + err.Error())
		}
		if len(dataBuf) != metadata.ObjSize {
			return errors.New("Decrypt produced too much data")
		}

		copy(buf, dataBuf)
	}
	return nil
}

func (dep dirEndpoint) FillAt(offset metadata.OffsetT, hint string, buf []byte) error {
	var totalBytes int
	var nbytes int
	var obj *os.File
	var e error

	if hint == "" {
		hint = metadata.DataToHash(buf)
	}

	dep.stats.Inc("writes")

	_, e = os.Stat(dep.baseDirectory + "/" + metadata.HashToObjID(hint))
	if !os.IsNotExist(e) {
		dep.stats.Inc("dedups")
		dep.mdChan <- metadata.NewEntry(offset, hint)
		return nil
	}

	dep.stats.Inc("uploads")

	obj, e = os.Create(dep.baseDirectory + "/" + metadata.HashToObjID(hint))
	if e != nil {
		return errors.New("Create " + e.Error())
	}
	defer obj.Close()

	// This should be changed to just use a pointer
	// to the buffer and encrypt if needed.  I'll do
	// that soon but just doing it very separately until
	// it works.
	if dep.key == nil {
		for totalBytes = 0; totalBytes < metadata.ObjSize; {
			nbytes, e = obj.Write(buf[totalBytes:metadata.ObjSize])
			if e != nil {
				return errors.New("Write: " + e.Error())
			}
			totalBytes += nbytes
		}
	} else {
		cipherBuf, err := encrypt.Encrypt(buf, *dep.key)
		if err != nil {
			return errors.New("Encrypt: " + e.Error())
		}

		length := len(cipherBuf)

		for totalBytes = 0; totalBytes < length; totalBytes += nbytes {
			nbytes, e = obj.Write(cipherBuf[totalBytes:length])
			if e != nil {
				return errors.New("Write: " + e.Error())
			}
		}
	}

	//fmt.Printf("Uploaded offset 0x%x\n", offset)
	dep.mdChan <- metadata.NewEntry(offset, hint)
	return nil
}

func (dep dirEndpoint) SkipAt(offset metadata.OffsetT, hint string) error {
	dep.stats.Inc("unchanged")
	dep.mdChan <- metadata.NewEntry(offset, hint)
	return nil
}

func (dep dirEndpoint) GetIncrSnapName() string {
	return dep.incrSnapName
}

func (dep dirEndpoint) GetLunSize() int64 {
	return dep.lunSize
}

func (dep dirEndpoint) Done(success bool) {
	switch dep.purpose {
	case source:
		// dep.baseReader.Close() or close the file?
		// dep.incrReader.Close() or close the file?
	case destination:
		// Close the mdChan to stop the metadata handler
		close(dep.mdChan)
		// Wait for it to finish
		dep.mdwg.Wait()
	default:
		fmt.Printf("Done on a bad Endpoint.")
	}
	dep.stats.Report("Directory " + dep.purpose.String())
}

// Make sure the directory for a particular file exists
func createDirs(path string) error {
	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return errors.New("MkdirAll: " + err.Error())
	}

	return nil
}

// Directory Metadata Handler
func dirMetadataHandler(dep *dirEndpoint) {
	var base metadata.OffsetT
	incrSnapObjectName := metadata.SnapMetadataName(dep.esName,
		dep.volName,
		dep.incrSnapName)
	var tmpFile = dep.baseDirectory + "/" + incrSnapObjectName + ".tmp"

	defer dep.mdwg.Done()

	mdmap := make(map[metadata.OffsetT]*metadata.Entry)

	e := createDirs(tmpFile)
	if e != nil {
		fmt.Printf("Creating Directories for %s Error: %s\n", tmpFile, e.Error())
		return
	}

	f, e := os.Create(tmpFile)
	if e != nil {
		fmt.Printf("Metadata: %s Error: %s\n", dep.incrSnapName, e.Error())
		return
	}
	defer os.Remove(tmpFile)

	// Put the header on the front of the file
	f.WriteString(metadata.Header() + "\n")

	for newmde := range dep.mdChan {
		mdmap[newmde.Offset()] = newmde

		mde, ok := mdmap[base]
		for ok {
			mdeLine := mde.Encode(dep.key)
			_, e = f.WriteString(mdeLine + "\n")
			if e != nil {
				fmt.Printf("Write to metadata failed\n")
				return
			}
			delete(mdmap, mde.Offset())
			base += metadata.ObjSize
			mde, ok = mdmap[base]
		}
	}

	e = os.Rename(tmpFile, dep.baseDirectory+"/"+incrSnapObjectName)
	if e != nil {
		fmt.Printf("Put of metadata file failed: File: %s to %s Error: %s\n",
			tmpFile, dep.baseDirectory+"/"+incrSnapObjectName, e.Error())
	}
}
