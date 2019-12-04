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
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/Nuvoloso/extentstore/encrypt"
	"github.com/Nuvoloso/extentstore/metadata"
	"github.com/Nuvoloso/extentstore/stats"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"os"
	"strings"
	"sync"
)

type gcEndpoint struct {
	purpose      Purpose
	bucketName   string
	esName       string
	volName      string
	baseSnapName string
	incrSnapName string
	baseReader   *csv.Reader
	incrReader   *csv.Reader
	mdChan       chan *metadata.Entry
	mdwg         *sync.WaitGroup
	lunSize      int64
	client       *storage.Client
	bucket       *storage.BucketHandle
	stats        *stats.Stats
	key          *[]byte
}

var gcUseEncryption bool = true

func (gcep *gcEndpoint) Config(p Purpose, arg string, workerCount uint) error {
	// break up the argument string
	// set the state
	var err error

	gcep.purpose = p
	gcep.stats = &stats.Stats{}
	gcep.stats.Init()

	fields := strings.Split(arg, ",")
	if len(fields) != 5 {
		return errors.New("Google Args <bucketname>,<esname>,<volume>,<basesnap>,<incrsnap>")
	}
	gcep.bucketName = fields[0]
	gcep.esName = fields[1]
	gcep.volName = fields[2]
	gcep.baseSnapName = fields[3]
	gcep.incrSnapName = fields[4]

	//create a client

	jsonPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")

	if len(jsonPath) == 0 {
		return errors.New("GOOGLE_APPLICATION_CREDENTIALS not set")
	}

	ctx := context.Background()
	gcep.client, err = storage.NewClient(ctx, option.WithCredentialsFile(jsonPath))
	if err != nil {
		return errors.New("Google client: " + err.Error())
	}

	// create a bucket handler
	gcep.bucket = gcep.client.Bucket(gcep.bucketName)

	switch p {
	case destination:
		// We need a metadata handler for a destination
		if gcep.baseSnapName != "" {
			baseSnapObjectName := metadata.SnapMetadataName(gcep.esName,
				gcep.volName,
				gcep.baseSnapName)
			present, err := gcep.objExists(baseSnapObjectName)
			if err != nil {
				return errors.New("Exists: " + err.Error())
			}
			if !present {
				return errors.New("Base Snapshot Not Present")
			}
		}
		gcep.mdwg = &sync.WaitGroup{}
		gcep.mdwg.Add(1)
		gcep.mdChan = make(chan *metadata.Entry, workerCount*2)
		go gcMetadataHandler(gcep)
		gcep.stats.Set("writes", 0)
		gcep.stats.Set("uploads", 0)
	case source:
		if gcep.baseSnapName == "" {
			gcep.baseReader, err = metadata.NewMetaReader("/dev/null")
			if err != nil {
				return errors.New("Can't setup null snapshot reader " + err.Error())
			}
		} else {
			baseSnapObjectName := metadata.SnapMetadataName(gcep.esName,
				gcep.volName,
				gcep.baseSnapName)
			err = gcep.getObjectFile(baseSnapObjectName, "/tmp/base")
			if err != nil {
				return errors.New("Can't retrieve base snapshot " + err.Error())
			}
			gcep.baseReader, err = metadata.NewMetaReader("/tmp/base")
			if err != nil {
				return errors.New("Can't setup base snapshot reader " + err.Error())
			}
		}
		incrSnapObjectName := metadata.SnapMetadataName(gcep.esName,
			gcep.volName,
			gcep.incrSnapName)
		err = gcep.getObjectFile(incrSnapObjectName, "/tmp/incr")
		if err != nil {
			return errors.New("Can't retrieve incr snapshot " + err.Error())
		}
		gcep.incrReader, err = metadata.NewMetaReader("/tmp/incr")
		if err != nil {
			return errors.New("Can't setup incremental snapshot reader " + err.Error())
		}
		gcep.stats.Set("clean", 0)
		gcep.stats.Set("dirty", 0)
		gcep.stats.Set("reads", 0)
	default:
		return errors.New("Invalid Purpose for Dir Endpoint")
	}

	if gcUseEncryption {
		key := []byte{
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
		}
		gcep.key = &key
	}

	var baseName string
	if gcep.baseSnapName == "" {
		baseName = "null"
	} else {
		baseName = gcep.baseSnapName
	}
	fmt.Printf("%s Google Endpoint on %s: %s %s %s -> %s\n", p.String(), gcep.bucketName,
		gcep.esName, gcep.volName, baseName, gcep.incrSnapName)

	return nil
}

// Could move this to metadata package
func (gcep gcEndpoint) Differ(dataChan chan *metadata.ChunkInfo, shouldAbort AbortCheck) error {
	defer close(dataChan)

	// should make the meta chan internal
	baseMDE := metadata.NextEntry(gcep.baseReader, gcep.key)
	incrMDE := metadata.NextEntry(gcep.incrReader, gcep.key)

	for incrMDE.Offset() != metadata.Infinity {
		if shouldAbort() {
			return errors.New("Stopped due to abort condition")
		}

		if incrMDE.Offset() == baseMDE.Offset() {
			if incrMDE.ObjID == baseMDE.ObjID {
				// No Difference
				gcep.stats.Inc("clean")
				dataChan <- &metadata.ChunkInfo{
					Offset: incrMDE.Offset(),
					Length: metadata.ObjSize,
					Hint:   incrMDE.ObjID,
					Dirty:  false,
				}
			} else {
				// Difference
				gcep.stats.Inc("dirty")
				dataChan <- &metadata.ChunkInfo{
					Offset: incrMDE.Offset(),
					Length: metadata.ObjSize,
					Hint:   incrMDE.ObjID,
					Dirty:  true,
				}
			}
			baseMDE = metadata.NextEntry(gcep.baseReader, gcep.key)
			incrMDE = metadata.NextEntry(gcep.incrReader, gcep.key)
		} else if incrMDE.Offset() < baseMDE.Offset() {
			// Difference
			gcep.stats.Inc("dirty")
			dataChan <- &metadata.ChunkInfo{
				Offset: incrMDE.Offset(),
				Length: metadata.ObjSize,
				Hint:   incrMDE.ObjID,
				Dirty:  true,
			}
			incrMDE = metadata.NextEntry(gcep.incrReader, gcep.key)
		} else if incrMDE.Offset() > baseMDE.Offset() {
			// How can this happen?
			// Since fixed Span Size not a diff
			baseMDE = metadata.NextEntry(gcep.baseReader, gcep.key)
		}
	}

	return nil
}

func dumpBuf(buf []byte) {
	for b := 0; b < len(buf); b++ {
		if 0 == b%16 {
			fmt.Printf("\n%07x", b)
		}
		fmt.Printf(" %02x", buf[b])
	}
	fmt.Printf("\n")
}

func (gcep gcEndpoint) FetchAt(offset metadata.OffsetT, hint string, buf []byte) error {
	if hint == "" {
		return errors.New("Hint Not Set")
	}

	// There is too much data copy here
	gcep.stats.Inc("reads")

	objName := metadata.HashToObjID(hint)

	var err error

	obj := gcep.bucket.Object(objName)
	ctx := context.TODO()
	r, err := obj.NewReader(ctx)
	if err != nil {
		return errors.New("NewReader: " + err.Error())
	}
	defer r.Close()

	if gcep.key == nil {
		var totalBytes int64
		var bytes int

		for totalBytes = 0; totalBytes < metadata.ObjSize; {
			bytes, err = r.Read(buf[totalBytes:])

			if err != nil && err != io.EOF {
				return errors.New("Read " + err.Error())
			}
			totalBytes += int64(bytes)
		}
	} else {
		ebuf := make([]byte, metadata.ObjSize+1024)

		var totalBytes int64
		var bytes int

		for totalBytes = 0; totalBytes < metadata.ObjSize; {
			bytes, err = r.Read(ebuf[totalBytes:])

			if err != nil && err != io.EOF {
				return errors.New("Read " + err.Error())
			}
			totalBytes += int64(bytes)
		}

		dataBuf, err := encrypt.Decrypt(ebuf[:totalBytes], *gcep.key)
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

func (gcep gcEndpoint) FillAt(offset metadata.OffsetT, hint string, buf []byte) error {
	if hint == "" {
		hint = metadata.DataToHash(buf)
	}

	objName := metadata.HashToObjID(hint)

	gcep.stats.Inc("writes")
	present, err := gcep.objExists(objName)
	if err != nil {
		return errors.New("Exists: " + err.Error())
	}
	if present {
		gcep.stats.Inc("dedups")
		gcep.mdChan <- metadata.NewEntry(offset, hint)
		return nil
	}
	gcep.stats.Inc("uploads")

	obj := gcep.bucket.Object(objName)
	ctx := context.TODO()
	w := obj.NewWriter(ctx)
	defer w.Close()

	if gcep.key == nil {
		buffer := bytes.NewBuffer(buf)

		n, err := io.Copy(w, buffer)
		if err != nil {
			return errors.New("Copy " + err.Error())
		}
		fmt.Printf("Uploaded %d bytes\n", n)
	} else {
		cipherBuf, err := encrypt.Encrypt(buf, *gcep.key)
		if err != nil {
			return errors.New("Encrypt: " + err.Error())
		}
		buffer := bytes.NewBuffer(cipherBuf)
		_, err = io.Copy(w, buffer)
		if err != nil {
			return errors.New("Copy: " + err.Error())
		}
	}

	//fmt.Printf("Uploaded offset 0x%x\n", offset)
	gcep.mdChan <- metadata.NewEntry(offset, hint)

	return nil
}

func (gcep gcEndpoint) SkipAt(offset metadata.OffsetT, hint string) error {
	gcep.stats.Inc("Skips")
	gcep.mdChan <- metadata.NewEntry(offset, hint)
	return nil
}

func (gcep gcEndpoint) GetIncrSnapName() string {
	return gcep.incrSnapName
}

func (gcep gcEndpoint) GetLunSize() int64 {
	return gcep.lunSize
}

func (gcep gcEndpoint) Done(success bool) {
	switch gcep.purpose {
	case source:
		// gcep.baseReader.Close() or close the file?
		// gcep.incrReader.Close() or close the file?
	case destination:
		// Close the mdChan to stop the metadata handler
		close(gcep.mdChan)
		// Wait for it to finish
		gcep.mdwg.Wait()
	default:
		fmt.Printf("Done on a bad Endpoint.")
	}
	gcep.stats.Report("S3 " + gcep.purpose.String())
}

func (gcep gcEndpoint) getObjectFile(ObjName string, fname string) error {
	f, err := os.Create(fname)
	if err != nil {
		return errors.New("Create " + err.Error())
	}
	defer f.Close()

	w := io.Writer(f)

	obj := gcep.bucket.Object(ObjName)
	ctx := context.TODO()
	r, err := obj.NewReader(ctx)
	if err != nil {
		return errors.New("NewWriter " + err.Error())
	}
	defer r.Close()

	_, err = io.Copy(w, r)
	if err != nil {
		return errors.New("Copy " + err.Error())
	}

	return nil
}

func (gcep gcEndpoint) putObjectFile(fname string, ObjName string) error {
	f, err := os.Open(fname)
	r := io.Reader(f)

	obj := gcep.bucket.Object(ObjName)
	ctx := context.TODO()
	w := obj.NewWriter(ctx)
	defer w.Close()

	_, err = io.Copy(w, r)
	if err != nil {
		return errors.New("Copy " + err.Error())
	}

	return nil
}

func (gcep gcEndpoint) objExists(objName string) (bool, error) {
	ctx := context.Background()

	q := &storage.Query{Prefix: objName}
	it := gcep.bucket.Objects(ctx, q)

	_, e := it.Next()

	if e != nil {
		if e != iterator.Done {
			return false, errors.New("Iterator: " + e.Error())
		}
		return false, nil
	}
	return true, nil
}

// Directory Metadata Handler
func gcMetadataHandler(gcep *gcEndpoint) {
	var base metadata.OffsetT
	var tmpFile = "/tmp/" + gcep.incrSnapName + ".tmp"

	defer gcep.mdwg.Done()

	mdmap := make(map[metadata.OffsetT]*metadata.Entry)

	f, e := os.Create(tmpFile)
	if e != nil {
		fmt.Printf("Metadata: %s Error: %s\n", gcep.incrSnapName, e.Error())
		return
	}
	defer os.Remove(tmpFile)

	// Put the header on the front of the file
	f.WriteString(metadata.Header() + "\n")

	for newmde := range gcep.mdChan {
		mdmap[newmde.Offset()] = newmde

		mde, ok := mdmap[base]
		for ok {
			mdeLine := mde.Encode(gcep.key)
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

	incrSnapObjectName := metadata.SnapMetadataName(gcep.esName,
		gcep.volName,
		gcep.incrSnapName)
	e = gcep.putObjectFile(tmpFile, incrSnapObjectName)
	if e != nil {
		fmt.Printf("Put of metadata file failed: File: %s to %s Error: %s\n", tmpFile, incrSnapObjectName, e.Error())
	}
}
