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
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/Nuvoloso/extentstore/encrypt"
	"github.com/Nuvoloso/extentstore/metadata"
	"github.com/Nuvoloso/extentstore/stats"
	"github.com/azure/azure-storage-blob-go/2018-03-28/azblob"
	"net/url"
	"os"
	"strings"
	"sync"
)

type azEndpoint struct {
	purpose      Purpose              // Source or Destination
	bucketName   string               // The bucket in the object store
	esName       string               // Extent Store Name
	volName      string               // Volume Series Name
	baseSnapName string               // Base Snapshot Name
	incrSnapName string               // Incremental Snapshot Name
	baseReader   *csv.Reader          // Reader for Base Snapshot Metadata
	incrReader   *csv.Reader          // Reader for Incremental Snapshot Metadata
	mdChan       chan *metadata.Entry // Metadata Channel used for collecting Metadata changes
	mdwg         *sync.WaitGroup      // Waitgroup to wait for go routines to finish
	lunSize      int64                // Size of the lun
	stats        *stats.Stats         // Collector of Statistics
	containerURL azblob.ContainerURL  // Accessors to Azure
	key          *[]byte              // Encryption Key (nil if encryption not desired)
}

var azUseEncryption bool = true

func (azep *azEndpoint) Config(p Purpose, arg string, workerCount uint) error {
	// break up the argument string
	// set the state
	var err error

	azep.purpose = p
	azep.stats = &stats.Stats{}
	azep.stats.Init()

	fields := strings.Split(arg, ",")
	if len(fields) != 5 {
		return errors.New("Azure Args <bucketname>,<esname>,<volume>,<basesnap>,<incrsnap>")
	}
	azep.bucketName = fields[0]
	azep.esName = fields[1]
	azep.volName = fields[2]
	azep.baseSnapName = fields[3]
	azep.incrSnapName = fields[4]

	// From the Azure portal, get your storage account name and key and set environment variables.
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		return errors.New("Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
	}

	credential := azblob.NewSharedKeyCredential(accountName, accountKey)
	pl := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, azep.bucketName))

	azep.containerURL = azblob.NewContainerURL(*URL, pl)

	switch p {
	case destination:
		if azep.baseSnapName != "" {
			baseSnapObjectName := metadata.SnapMetadataName(azep.esName,
				azep.volName,
				azep.baseSnapName)
			present, err := azep.objExists(baseSnapObjectName)
			if err != nil {
				return errors.New("Exists: " + err.Error())
			}
			if !present {
				return errors.New("Base Snapshot Not Present")
			}
		}
		// We need a metadata handler for a destination
		azep.mdwg = &sync.WaitGroup{}
		azep.mdwg.Add(1)
		azep.mdChan = make(chan *metadata.Entry, workerCount*2)
		go azMetadataHandler(azep)
		azep.stats.Set("writes", 0)
		azep.stats.Set("uploads", 0)
	case source:
		if azep.baseSnapName == "" {
			azep.baseReader, err = metadata.NewMetaReader("/dev/null")
			if err != nil {
				return errors.New("Can't setup null snapshot reader " + err.Error())
			}
		} else {
			baseSnapObjectName := metadata.SnapMetadataName(azep.esName,
				azep.volName,
				azep.baseSnapName)
			err = azep.getObjectFile(baseSnapObjectName, "/tmp/base")
			if err != nil {
				return errors.New("Can't retrieve base snapshot " + err.Error())
			}
			azep.baseReader, err = metadata.NewMetaReader("/tmp/base")
			if err != nil {
				return errors.New("Can't setup base snapshot reader " + err.Error())
			}
		}
		incrSnapObjectName := metadata.SnapMetadataName(azep.esName,
			azep.volName,
			azep.incrSnapName)
		err = azep.getObjectFile(incrSnapObjectName, "/tmp/incr")
		if err != nil {
			return errors.New("Can't retrieve incr snapshot " + err.Error())
		}
		azep.incrReader, err = metadata.NewMetaReader("/tmp/incr")
		if err != nil {
			return errors.New("Can't setup incremental snapshot reader " + err.Error())
		}
		azep.stats.Set("clean", 0)
		azep.stats.Set("dirty", 0)
		azep.stats.Set("reads", 0)
	default:
		return errors.New("Invalid Purpose for Dir Endpoint")
	}

	if azUseEncryption {
		key := []byte{
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
		}
		azep.key = &key
	}

	var baseName string
	if azep.baseSnapName == "" {
		baseName = "null"
	} else {
		baseName = azep.baseSnapName
	}
	fmt.Printf("%s Azure Endpoint on %s: %s %s, %s -> %s\n", p.String(),
		azep.bucketName, azep.esName, azep.volName, baseName, azep.incrSnapName)

	return nil
}

// Could move this to metadata package
func (azep azEndpoint) Differ(dataChan chan *metadata.ChunkInfo, shouldAbort AbortCheck) error {
	defer close(dataChan)

	// should make the meta chan internal
	baseMDE := metadata.NextEntry(azep.baseReader, azep.key)
	incrMDE := metadata.NextEntry(azep.incrReader, azep.key)

	for incrMDE.Offset() != metadata.Infinity {
		if shouldAbort() {
			return errors.New("Stopped due to abort condition")
		}

		if incrMDE.Offset() == baseMDE.Offset() {
			if incrMDE.ObjID == baseMDE.ObjID {
				// No Difference
				azep.stats.Inc("clean")
				dataChan <- &metadata.ChunkInfo{
					Offset: incrMDE.Offset(),
					Length: metadata.ObjSize,
					Hint:   incrMDE.ObjID,
					Dirty:  false,
				}
			} else {
				// Difference
				azep.stats.Inc("dirty")
				dataChan <- &metadata.ChunkInfo{
					Offset: incrMDE.Offset(),
					Length: metadata.ObjSize,
					Hint:   incrMDE.ObjID,
					Dirty:  true,
				}
			}
			baseMDE = metadata.NextEntry(azep.baseReader, azep.key)
			incrMDE = metadata.NextEntry(azep.incrReader, azep.key)
		} else if incrMDE.Offset() < baseMDE.Offset() {
			// Difference
			azep.stats.Inc("dirty")
			dataChan <- &metadata.ChunkInfo{
				Offset: incrMDE.Offset(),
				Length: metadata.ObjSize,
				Hint:   incrMDE.ObjID,
				Dirty:  true,
			}
			incrMDE = metadata.NextEntry(azep.incrReader, azep.key)
		} else if incrMDE.Offset() > baseMDE.Offset() {
			// How can this happen?
			// Since fixed Span Size not a diff
			baseMDE = metadata.NextEntry(azep.baseReader, azep.key)
		}
	}

	return nil
}

func (azep azEndpoint) FetchAt(offset metadata.OffsetT, hint string, buf []byte) error {
	if hint == "" {
		return errors.New("Hint Not Set")
	}

	// There is too much data copy here
	azep.stats.Inc("reads")

	objName := metadata.HashToObjID(hint)

	var err error

	ctx := context.Background()
	blockBlobURL := azep.containerURL.NewBlockBlobURL(objName)

	if azep.key == nil {
		err = azblob.DownloadBlobToBuffer(ctx, blockBlobURL.BlobURL, int64(0), int64(0),
			azblob.BlobAccessConditions{}, buf, azblob.DownloadFromBlobOptions{
				BlockSize:   int64(1 * 1024 * 1024),
				Parallelism: uint16(2)})

		if err != nil {
			return errors.New("Download: " + err.Error())
		}
	} else {
		ebuf := make([]byte, metadata.ObjSize+28) // XXX TODO HACK I shouldn't know this size

		err = azblob.DownloadBlobToBuffer(ctx, blockBlobURL.BlobURL, int64(0), int64(0),
			azblob.BlobAccessConditions{}, ebuf, azblob.DownloadFromBlobOptions{
				BlockSize:   int64(2 * 1024 * 1024),
				Parallelism: uint16(2)})
		if err != nil {
			return errors.New("Download: " + err.Error())
		}

		dataBuf, err := encrypt.Decrypt(ebuf, *azep.key)
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

func (azep azEndpoint) FillAt(offset metadata.OffsetT, hint string, buf []byte) error {
	if hint == "" {
		hint = metadata.DataToHash(buf)
	}

	objName := metadata.HashToObjID(hint)

	azep.stats.Inc("writes")

	present, err := azep.objExists(objName)
	if err != nil {
		return errors.New("Exists: " + err.Error())
	}
	if present {
		azep.stats.Inc("dedups")
		azep.mdChan <- metadata.NewEntry(offset, hint)
		return nil
	}
	azep.stats.Inc("uploads")

	if azep.key == nil {
		ctx := context.Background()
		blobURL := azep.containerURL.NewBlockBlobURL(objName)

		_, err = azblob.UploadBufferToBlockBlob(ctx, buf,
			blobURL, azblob.UploadToBlockBlobOptions{
				BlockSize:   2 * 1024 * 1024,
				Parallelism: 16})
		if err != nil {
			return errors.New("Upload: " + err.Error())
		}
	} else {
		cipherBuf, err := encrypt.Encrypt(buf, *azep.key)
		if err != nil {
			return errors.New("Encrypt: " + err.Error())
		}

		ctx := context.Background()
		blobURL := azep.containerURL.NewBlockBlobURL(objName)

		_, err = azblob.UploadBufferToBlockBlob(ctx, cipherBuf,
			blobURL, azblob.UploadToBlockBlobOptions{
				BlockSize:   2 * 1024 * 1024,
				Parallelism: 16})
		if err != nil {
			return errors.New("Upload: " + err.Error())
		}
	}

	azep.mdChan <- metadata.NewEntry(offset, hint)

	return nil
}

func (azep azEndpoint) SkipAt(offset metadata.OffsetT, hint string) error {
	azep.stats.Inc("Skips")
	azep.mdChan <- metadata.NewEntry(offset, hint)
	return nil
}

func (azep azEndpoint) GetIncrSnapName() string {
	return azep.incrSnapName
}

func (azep azEndpoint) GetLunSize() int64 {
	return azep.lunSize
}

func (azep azEndpoint) Done(success bool) {
	switch azep.purpose {
	case source:
		// azep.baseReader.Close() or close the file?
		// azep.incrReader.Close() or close the file?
	case destination:
		// Close the mdChan to stop the metadata handler
		close(azep.mdChan)
		// Wait for it to finish
		azep.mdwg.Wait()
	default:
		fmt.Printf("Done on a bad Endpoint.")
	}
	azep.stats.Report("S3 " + azep.purpose.String())
}

func (azep azEndpoint) getObjectFile(ObjName string, fname string) error {
	var err error

	file, err := os.Create(fname)
	if err != nil {
		return errors.New("Create " + err.Error())
	}
	defer file.Close()

	ctx := context.Background()

	blockBlobURL := azep.containerURL.NewBlockBlobURL(ObjName)

	err = azblob.DownloadBlobToFile(ctx, blockBlobURL.BlobURL, int64(0), int64(0),
		azblob.BlobAccessConditions{}, file, azblob.DownloadFromBlobOptions{})

	if err != nil {
		return errors.New("DownloadBlobToFile: " + err.Error())
	}

	return nil
}

func (azep azEndpoint) putObjectFile(fname string, ObjName string) error {

	file, err := os.Open(fname)
	if err != nil {
		return errors.New("Open: " + err.Error())
	}
	defer file.Close()

	ctx := context.Background() // This example uses a never-expiring context
	blobURL := azep.containerURL.NewBlockBlobURL(ObjName)

	_, err = azblob.UploadFileToBlockBlob(ctx, file, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   2 * 1024 * 1024,
		Parallelism: 16})

	if err != nil {
		return errors.New("Upload: " + err.Error())
	}

	return nil
}

func (azep azEndpoint) objExists(objName string) (bool, error) {
	ctx := context.Background()

	res, err := azep.containerURL.ListBlobsFlatSegment(ctx, azblob.Marker{},
		azblob.ListBlobsSegmentOptions{Prefix: objName, MaxResults: 1})
	if err != nil {
		return false, errors.New("List: " + err.Error())
	}

	if len(res.Segment.BlobItems) == 1 {
		return true, nil
	} else {
		return false, nil
	}
}

// Directory Metadata Handler
func azMetadataHandler(azep *azEndpoint) {
	var base metadata.OffsetT
	var tmpFile = "/tmp/" + azep.incrSnapName + ".tmp"

	defer azep.mdwg.Done()

	mdmap := make(map[metadata.OffsetT]*metadata.Entry)

	f, e := os.Create(tmpFile)
	if e != nil {
		fmt.Printf("Metadata: %s Error: %s\n", azep.incrSnapName, e.Error())
		return
	}
	defer os.Remove(tmpFile)

	// Put the header on the front of the file
	f.WriteString(metadata.Header() + "\n")

	for newmde := range azep.mdChan {
		mdmap[newmde.Offset()] = newmde

		mde, ok := mdmap[base]
		for ok {
			mdeLine := mde.Encode(azep.key)
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

	incrSnapObjectName := metadata.SnapMetadataName(azep.esName,
		azep.volName,
		azep.incrSnapName)
	e = azep.putObjectFile(tmpFile, incrSnapObjectName)
	if e != nil {
		fmt.Printf("Put of metadata file failed: File: %s to %s Error: %s\n", tmpFile, azep.incrSnapName, e.Error())
	}
}
