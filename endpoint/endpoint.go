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
	"flag"
	"github.com/Nuvoloso/extentstore/metadata"
)

// What type of source is being backed up?
var srcType string
var srcArg string
var dstType string
var dstArg string

// Purpose is the reason for the endpoint: source or Destination
type Purpose int

const (
	// Source Endpoint
	source Purpose = iota
	// Destination Endpoint
	destination
)

func (p Purpose) String() string {
	switch p {
	case source:
		return "Source"
	case destination:
		return "Destination"
	default:
		return "Unknown"
	}
}

// AbortCheck is the type of function used to determine
// if processing should abort.
type AbortCheck func() bool

func init() {
	// These args could be combined into src and dst with the rest comma separated
	flag.StringVar(&srcType, "srctype", "file", "source endpoint type (dir,file)")
	flag.StringVar(&srcArg, "srcarg", "lun", "source endpoint arguments")
	flag.StringVar(&dstType, "dsttype", "dir", "destination endpoint type (dir,file)")
	flag.StringVar(&dstArg, "dstarg", "/tmp/extentstore,null,snap", "destnation endpoint arguments")
}

// EndPoint is the interface all endpoints should implement
type EndPoint interface {
	Differ(dataChan chan *metadata.ChunkInfo, shouldAbort AbortCheck) error
	FetchAt(offset metadata.OffsetT, hint string, buf []byte) error
	FillAt(offset metadata.OffsetT, hint string, buf []byte) error
	SkipAt(offset metadata.OffsetT, hint string) error
	GetIncrSnapName() string
	GetLunSize() int64
	Done(success bool)
}

// SetupEndpoints interprets the command line arguments and creates
// a source and destination endpoint for each.
func SetupEndpoints(workerCount uint) (EndPoint, EndPoint, error) {
	var srcEp EndPoint
	var dstEp EndPoint

	switch srcType {
	case "file":
		fep := fileEndpoint{}
		e := fep.Config(source, srcArg)
		if e != nil {
			return nil, nil, errors.New("Source File Endpoint " + e.Error())
		}
		srcEp = fep
	case "dir":
		dep := dirEndpoint{}
		e := dep.Config(source, srcArg, workerCount)
		if e != nil {
			return nil, nil, errors.New("Source Directory Endpoint " + e.Error())
		}
		srcEp = dep
	case "s3":
		s3ep := s3Endpoint{}
		e := s3ep.Config(source, srcArg, workerCount)
		if e != nil {
			return nil, nil, errors.New("Source S3 Endpoint " + e.Error())
		}
		srcEp = s3ep
	case "nuvo":
		nep := nuvoEndpoint{}
		e := nep.Config(source, srcArg)
		if e != nil {
			return nil, nil, errors.New("Source Nuvo Endpoint " + e.Error())
		}
		srcEp = nep
	case "null":
		nep := nullEndpoint{}
		e := nep.Config(source, srcArg)
		if e != nil {
			return nil, nil, errors.New("Source Null Endpoint " + e.Error())
		}
		srcEp = nep
	case "gc":
		gcep := gcEndpoint{}
		e := gcep.Config(source, srcArg, workerCount)
		if e != nil {
			return nil, nil, errors.New("Source Google Endpoint " + e.Error())
		}
		srcEp = gcep
	case "az":
		azep := azEndpoint{}
		e := azep.Config(source, srcArg, workerCount)
		if e != nil {
			return nil, nil, errors.New("Source Azure Endpoint " + e.Error())
		}
		srcEp = azep
	default:
		return nil, nil, errors.New("Unknown Source Endpoint Type")
	}

	switch dstType {
	case "file":
		fep := fileEndpoint{}
		e := fep.Config(destination, dstArg)
		if e != nil {
			return nil, nil, errors.New("Destination File Endpoint " + e.Error())
		}
		dstEp = fep
	case "dir":
		dep := dirEndpoint{}
		e := dep.Config(destination, dstArg, workerCount)
		if e != nil {
			return nil, nil, errors.New("Destination Directory Endpoint " + e.Error())
		}
		dstEp = dep
	case "s3":
		s3ep := s3Endpoint{}
		e := s3ep.Config(destination, dstArg, workerCount)
		if e != nil {
			return nil, nil, errors.New("Destination S3 Endpoint " + e.Error())
		}
		dstEp = s3ep
	case "nuvo":
		nep := nuvoEndpoint{}
		e := nep.Config(destination, dstArg)
		if e != nil {
			return nil, nil, errors.New("Destination Nuvo Endpoint " + e.Error())
		}
		dstEp = nep
	case "null":
		nep := nullEndpoint{}
		e := nep.Config(destination, dstArg)
		if e != nil {
			return nil, nil, errors.New("Destination Null Endpoint " + e.Error())
		}
		dstEp = nep
	case "gc":
		gcep := gcEndpoint{}
		e := gcep.Config(destination, dstArg, workerCount)
		if e != nil {
			return nil, nil, errors.New("Destination Google Endpoint " + e.Error())
		}
		dstEp = gcep
	case "az":
		azep := azEndpoint{}
		e := azep.Config(destination, dstArg, workerCount)
		if e != nil {
			return nil, nil, errors.New("Destination Google Endpoint " + e.Error())
		}
		dstEp = azep
	default:
		return nil, nil, errors.New("Unknown Destination Endpoint Type")
	}

	return srcEp, dstEp, nil
}
