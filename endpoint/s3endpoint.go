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
	//"github.com/aws/aws-sdk-go-v2/aws"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/Nuvoloso/extentstore/encrypt"
	"github.com/Nuvoloso/extentstore/metadata"
	"github.com/Nuvoloso/extentstore/stats"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"os"
	"strings"
	"sync"
)

type s3Endpoint struct {
	purpose      Purpose
	bucket       string
	esName       string
	volName      string
	baseSnapName string
	incrSnapName string
	baseReader   *csv.Reader
	incrReader   *csv.Reader
	mdChan       chan *metadata.Entry
	mdwg         *sync.WaitGroup
	lunSize      int64
	svc          *s3.S3
	stats        *stats.Stats
	key          *[]byte
}

var s3UseEncryption bool = true

func (s3ep *s3Endpoint) Config(p Purpose, arg string, workerCount uint) error {
	// break up the argument string
	// set the state
	var err error

	s3ep.purpose = p
	s3ep.stats = &stats.Stats{}
	s3ep.stats.Init()

	fields := strings.Split(arg, ",")
	if len(fields) != 6 {
		return errors.New("S3 Args <bucket>,<region>,<esname>,<volname>,<basesnap>,<incrsnap>")
	}
	s3ep.bucket = fields[0]
	s3ep.esName = fields[2]
	s3ep.volName = fields[3]
	s3ep.baseSnapName = fields[4]
	s3ep.incrSnapName = fields[5]

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return errors.New("Load AWS Config " + err.Error())
	}

	cfg.Region = fields[1]

	s3ep.svc = s3.New(cfg)

	switch p {
	case destination:
		if s3ep.baseSnapName != "" {
			baseSnapObjectName := metadata.SnapMetadataName(s3ep.esName,
				s3ep.volName,
				s3ep.baseSnapName)
			present, err := s3ep.objExists(baseSnapObjectName)
			if err != nil {
				return errors.New("Exists: " + err.Error())
			}
			if !present {
				return errors.New("Base Snapshot Not Present")
			}
		}
		// We need a metadata handler for a destination
		s3ep.mdwg = &sync.WaitGroup{}
		s3ep.mdwg.Add(1)
		s3ep.mdChan = make(chan *metadata.Entry, workerCount*2)
		go s3MetadataHandler(s3ep)
		s3ep.stats.Set("writes", 0)
		s3ep.stats.Set("uploads", 0)
	case source:
		if s3ep.baseSnapName == "" {
			s3ep.baseReader, err = metadata.NewMetaReader("/dev/null")
			if err != nil {
				return errors.New("Can't setup null snapshot reader " + err.Error())
			}
		} else {
			baseSnapObjectName := metadata.SnapMetadataName(s3ep.esName,
				s3ep.volName,
				s3ep.baseSnapName)
			err = s3ep.getObjectFile(baseSnapObjectName, "/tmp/base")
			if err != nil {
				return errors.New("Can't retrieve base snapshot " + err.Error())
			}
			s3ep.baseReader, err = metadata.NewMetaReader("/tmp/base")
			if err != nil {
				return errors.New("Can't setup base snapshot reader " + err.Error())
			}
		}
		incrSnapObjectName := metadata.SnapMetadataName(s3ep.esName,
			s3ep.volName,
			s3ep.incrSnapName)
		err = s3ep.getObjectFile(incrSnapObjectName, "/tmp/incr")
		if err != nil {
			return errors.New("Can't retrieve incr snapshot " + err.Error())
		}
		s3ep.incrReader, err = metadata.NewMetaReader("/tmp/incr")
		if err != nil {
			return errors.New("Can't setup incremental snapshot reader " + err.Error())
		}
		s3ep.stats.Set("clean", 0)
		s3ep.stats.Set("dirty", 0)
		s3ep.stats.Set("reads", 0)
	default:
		return errors.New("Invalid Purpose for Dir Endpoint")
	}

	if s3UseEncryption {
		key := []byte{
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
		}
		s3ep.key = &key
	}

	var baseName string
	if s3ep.baseSnapName == "" {
		baseName = "null"
	} else {
		baseName = s3ep.baseSnapName
	}
	fmt.Printf("%s S3 Endpoint on %s: %s %s %s -> %s\n", p.String(), s3ep.bucket,
		s3ep.esName, s3ep.volName, baseName, s3ep.incrSnapName)

	return nil
}

// Could move this to metadata package
func (s3ep s3Endpoint) Differ(dataChan chan *metadata.ChunkInfo, shouldAbort AbortCheck) error {
	defer close(dataChan)

	// should make the meta chan internal
	baseMDE := metadata.NextEntry(s3ep.baseReader, s3ep.key)
	incrMDE := metadata.NextEntry(s3ep.incrReader, s3ep.key)

	for incrMDE.Offset() != metadata.Infinity {
		if shouldAbort() {
			return errors.New("Stopped due to abort condition")
		}

		if incrMDE.Offset() == baseMDE.Offset() {
			if incrMDE.ObjID == baseMDE.ObjID {
				// No Difference
				s3ep.stats.Inc("clean")
				dataChan <- &metadata.ChunkInfo{
					Offset: incrMDE.Offset(),
					Length: metadata.ObjSize,
					Hint:   incrMDE.ObjID,
					Dirty:  false,
				}
			} else {
				// Difference
				s3ep.stats.Inc("dirty")
				dataChan <- &metadata.ChunkInfo{
					Offset: incrMDE.Offset(),
					Length: metadata.ObjSize,
					Hint:   incrMDE.ObjID,
					Dirty:  true,
				}
			}
			baseMDE = metadata.NextEntry(s3ep.baseReader, s3ep.key)
			incrMDE = metadata.NextEntry(s3ep.incrReader, s3ep.key)
		} else if incrMDE.Offset() < baseMDE.Offset() {
			// Difference
			s3ep.stats.Inc("dirty")
			dataChan <- &metadata.ChunkInfo{
				Offset: incrMDE.Offset(),
				Length: metadata.ObjSize,
				Hint:   incrMDE.ObjID,
				Dirty:  true,
			}
			incrMDE = metadata.NextEntry(s3ep.incrReader, s3ep.key)
		} else if incrMDE.Offset() > baseMDE.Offset() {
			// How can this happen?
			// Since fixed Span Size not a diff
			baseMDE = metadata.NextEntry(s3ep.baseReader, s3ep.key)
		}
	}

	return nil
}

func (s3ep s3Endpoint) FetchAt(offset metadata.OffsetT, hint string, buf []byte) error {
	if hint == "" {
		return errors.New("Hint Not Set")
	}

	s3ep.stats.Inc("reads")

	objName := metadata.HashToObjID(hint)

	params := &s3.GetObjectInput{
		Bucket: &s3ep.bucket,
		Key:    &objName,
		//SSECustomerAlgorithm: "AES256",
		//SSECustomerKey: "Federwisch",
	}

	req := s3ep.svc.GetObjectRequest(params)

	output, e := req.Send()
	if e != nil {
		return errors.New("Send: " + e.Error())
	}

	if s3ep.key == nil {
		var totalBytes int64
		var bytes int

		for totalBytes = 0; totalBytes < metadata.ObjSize; {
			bytes, e = output.Body.Read(buf[totalBytes:])

			if e != nil && e != io.EOF {
				return errors.New("Read " + e.Error())
			}
			totalBytes += int64(bytes)
		}
	} else {
		ebuf := make([]byte, metadata.ObjSize+1024)

		var totalBytes int64
		var bytes int

		for totalBytes = 0; totalBytes < metadata.ObjSize; {
			bytes, e = output.Body.Read(ebuf[totalBytes:])

			if e != nil && e != io.EOF {
				return errors.New("Read " + e.Error())
			}
			totalBytes += int64(bytes)
		}

		dataBuf, err := encrypt.Decrypt(ebuf[:totalBytes], *s3ep.key)
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

func (s3ep s3Endpoint) FillAt(offset metadata.OffsetT, hint string, buf []byte) error {
	if hint == "" {
		hint = metadata.DataToHash(buf)
	}

	objName := metadata.HashToObjID(hint)

	s3ep.stats.Inc("writes")

	present, err := s3ep.objExists(objName)
	if err != nil {
		return errors.New("Exists: " + err.Error())
	}
	if present {
		s3ep.stats.Inc("dedups")
		s3ep.mdChan <- metadata.NewEntry(offset, hint)
		return nil
	}
	s3ep.stats.Inc("uploads")

	var params *s3.PutObjectInput

	if s3ep.key == nil {
		params = &s3.PutObjectInput{
			Bucket: &s3ep.bucket,
			Key:    &objName,
			Body:   bytes.NewReader(buf),
		}
	} else {
		cipherBuf, err := encrypt.Encrypt(buf, *s3ep.key)
		if err != nil {
			return errors.New("Encrypt: " + err.Error())
		}

		length := len(cipherBuf)

		params = &s3.PutObjectInput{
			Bucket: &s3ep.bucket,
			Key:    &objName,
			Body:   bytes.NewReader(cipherBuf[:length]),
		}
	}

	req := s3ep.svc.PutObjectRequest(params)

	_, e := req.Send()
	if e != nil {
		return errors.New("Send: " + e.Error())
	}

	//fmt.Printf("Uploaded offset 0x%x\n", offset)
	s3ep.mdChan <- metadata.NewEntry(offset, hint)

	return nil
}

func (s3ep s3Endpoint) SkipAt(offset metadata.OffsetT, hint string) error {
	s3ep.stats.Inc("Skips")
	s3ep.mdChan <- metadata.NewEntry(offset, hint)
	return nil
}

func (s3ep s3Endpoint) GetIncrSnapName() string {
	return s3ep.incrSnapName
}

func (s3ep s3Endpoint) GetLunSize() int64 {
	return s3ep.lunSize
}

func (s3ep s3Endpoint) Done(success bool) {
	switch s3ep.purpose {
	case source:
		// s3ep.baseReader.Close() or close the file?
		// s3ep.incrReader.Close() or close the file?
	case destination:
		// Close the mdChan to stop the metadata handler
		close(s3ep.mdChan)
		// Wait for it to finish
		s3ep.mdwg.Wait()
	default:
		fmt.Printf("Done on a bad Endpoint.")
	}
	s3ep.stats.Report("S3 " + s3ep.purpose.String())
}

func (s3ep s3Endpoint) getObjectFile(ObjName string, fname string) error {
	params := &s3.GetObjectInput{
		Bucket: &s3ep.bucket,
		Key:    &ObjName,
		//SSECustomerAlgorithm: "AES256",
		//SSECustomerKey: "Federwisch",
	}

	req := s3ep.svc.GetObjectRequest(params)

	output, e := req.Send()
	if e != nil {
		return errors.New("getObjectFile Send " + e.Error())
	}

	var f *os.File

	f, e = os.Create(fname)
	if e != nil {
		return errors.New("getObjectFile Open " + e.Error())
	}
	_, e = io.Copy(f, output.Body)
	if e != nil {
		return errors.New("getObjectFile Copy " + e.Error())
	}

	f.Close()

	return e
}

func (s3ep s3Endpoint) putObjectFile(fname string, ObjName string) error {

	f, e := os.Open(fname)

	params := &s3.PutObjectInput{
		Bucket: &s3ep.bucket,
		Key:    &ObjName,
		Body:   f,
	}

	req := s3ep.svc.PutObjectRequest(params)

	_, e = req.Send()
	if e != nil {
		return errors.New("putObjectFile Send " + e.Error())
	}
	return nil
}

func (s3ep s3Endpoint) objExists(objName string) (bool, error) {
	var maxCount int64 = 2

	params := &s3.ListObjectsInput{
		Bucket:  &s3ep.bucket,
		Prefix:  &objName,
		MaxKeys: &maxCount,
	}

	req := s3ep.svc.ListObjectsRequest(params)

	output, e := req.Send()
	if e != nil {
		return false, errors.New("List: " + e.Error())
	}

	for _, o := range output.Contents {
		if *o.Key == objName {
			return true, nil
		}
	}

	return false, nil
}

// Directory Metadata Handler
func s3MetadataHandler(s3ep *s3Endpoint) {
	var base metadata.OffsetT
	var tmpFile = "/tmp/" + s3ep.incrSnapName + ".tmp"

	defer s3ep.mdwg.Done()

	mdmap := make(map[metadata.OffsetT]*metadata.Entry)

	f, e := os.Create(tmpFile)
	if e != nil {
		fmt.Printf("Metadata: %s Error: %s\n", s3ep.incrSnapName, e.Error())
		return
	}
	defer os.Remove(tmpFile)

	// Put the header on the front of the file
	f.WriteString(metadata.Header() + "\n")

	for newmde := range s3ep.mdChan {
		mdmap[newmde.Offset()] = newmde

		mde, ok := mdmap[base]
		for ok {
			mdeLine := mde.Encode(s3ep.key)
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

	incrSnapObjectName := metadata.SnapMetadataName(s3ep.esName,
		s3ep.volName,
		s3ep.incrSnapName)
	e = s3ep.putObjectFile(tmpFile, incrSnapObjectName)
	if e != nil {
		fmt.Printf("Put of metadata file failed: File: %s to %s Error: %s\n", tmpFile, s3ep.incrSnapName, e.Error())
	}
}
