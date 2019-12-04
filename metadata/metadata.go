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

package metadata

import (
	"crypto/sha256"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/Nuvoloso/extentstore/encrypt"
	"io"
	"os"
	"strconv"
)

// OffsetT is the type of a Volume offset
type OffsetT uint64

func (o OffsetT) String() string {
	return fmt.Sprintf("%x", o)
}

// ObjSize is the size of an object
const ObjSize = 1024 * 1024
const objSizeBits = 20
const lvlSizeBits OffsetT = 11

// Infinity is the maximum OffsetT possible
const Infinity = 0xFFFFFFFFFFFFFFFF

// Entry is an internal representation for an entry in the metadata representing one offset range
type Entry struct {
	L4     uint32
	L3     uint32
	L2     uint32
	L1     uint32
	ObjID  string
	offset OffsetT // Cache of calculated offset
}

// ChunkInfo is the description of a portion of the Volume being backed up
type ChunkInfo struct {
	Offset OffsetT
	Length OffsetT
	Dirty  bool
	Hash   string
	Hint   string
	Last   bool
}

func (ci ChunkInfo) String() string {
	var dirtyStr string

	if ci.Dirty {
		dirtyStr = "Dirty"
	} else {
		dirtyStr = "Clean"
	}
	return fmt.Sprintf("Offset: 0x%x Length: %d %s Hash: %s Hint: %s",
		ci.Offset, ci.Length, dirtyStr, ci.Hash, ci.Hint)
}

// Header generates the Header for a metadata file
func Header() string {
	return fmt.Sprintf("L4,L3,L2,L1,OBJID")
}

// Offset returns the offset of an Entry
func (m *Entry) Offset() OffsetT {
	return m.offset
}

// Encrypt the Object ID into something to put in the metadata file
func encryptObjName(objId string, key *[]byte) string {
	if key == nil {
		return objId
	}
	arr, err := encrypt.Encrypt([]byte(objId), *key)
	if err != nil {
		fmt.Printf("encrypt.Encrypt Error: " + err.Error())
	}

	return fmt.Sprintf("%x", arr)
}

// Decrypt the metadata object field into an Object ID
func decryptObjName(obj string, key *[]byte) string {
	if key == nil {
		return obj
	}

	var buf []byte

	_, e := fmt.Sscanf(obj, "%x", &buf)
	if e != nil {
		fmt.Println("ERROR: " + e.Error())
	}

	arr, err := encrypt.Decrypt(buf, *key)
	if err != nil {
		fmt.Printf("encrypt.Decrypt Error: " + err.Error() + "\n")
	}

	return fmt.Sprintf("%s", arr)
}

// Encode creates a metadata file line for this entry
func (m *Entry) Encode(key *[]byte) string {
	return fmt.Sprintf("%x,%x,%x,%x,%s", m.L4, m.L3, m.L2, m.L1, encryptObjName(m.ObjID, key))
}

// fieldsToMetadata allocates a new metadata entry from a set of fields
func fieldsToMetadataEntry(field []string, key *[]byte) *Entry {
	var e error
	var l uint64

	l, e = strconv.ParseUint(field[0], 16, 11)
	if e != nil {
		return nil
	}
	l4 := uint32(l)
	l, e = strconv.ParseUint(field[1], 16, 11)
	if e != nil {
		return nil
	}
	l3 := uint32(l)
	l, e = strconv.ParseUint(field[2], 16, 11)
	if e != nil {
		return nil
	}
	l2 := uint32(l)
	l, e = strconv.ParseUint(field[3], 16, 11)
	if e != nil {
		return nil
	}
	l1 := uint32(l)

	off := OffsetT((l4 & 0x3FF))
	off = (off << OffsetT(lvlSizeBits)) | (OffsetT(l3) & 0x3FF)
	off = (off << OffsetT(lvlSizeBits)) | (OffsetT(l2) & 0x3FF)
	off = (off << OffsetT(lvlSizeBits)) | (OffsetT(l1) & 0x3FF)
	off <<= objSizeBits

	objId := decryptObjName(field[4], key)

	return &Entry{
		L4:     l4,
		L3:     l3,
		L2:     l2,
		L1:     l1,
		ObjID:  objId,
		offset: off,
	}
}

// NewEntry allocates a new metadata entry from offset and object ID
func NewEntry(offset OffsetT, objname string) *Entry {
	off := uint32(offset >> objSizeBits)
	L1 := off & 0x3FF
	off >>= lvlSizeBits
	L2 := off & 0x3FF
	off >>= lvlSizeBits
	L3 := off & 0x3FF
	off >>= lvlSizeBits
	L4 := off & 0x3FF

	return &Entry{L4, L3, L2, L1, objname, offset}
}

// The layout in the object store is the same for each endpoing
func SnapMetadataName(es string, vs string, sn string) string {
	return es + "/" + vs + "/" + sn
}

// HashToObjID returns and object name from a data hash
func HashToObjID(hash string) string {
	return "data/" + hash
}

func (m *Entry) String() string {
	return fmt.Sprintf("%x,%x,%x,%x,%s", m.L4, m.L3, m.L2, m.L1, m.ObjID)
}

// NewMetaReader creates a reader to use for parsing metadata files
func NewMetaReader(metadatafn string) (*csv.Reader, error) {
	mdf, e := os.Open(metadatafn)
	if e != nil {
		return nil, errors.New("Metadata file Header open failed: " + e.Error())
	}
	r := csv.NewReader(mdf)

	// Ignore the header
	fields, e := r.Read()
	// Assert fields[0] == "L4"
	if e != io.EOF && fields[0] != "L4" {
		return nil, errors.New("Metadata file Header Problem: " + e.Error())
	}
	return r, nil
}

// NextEntry returns an Entry representing the next line in the metadata file
func NextEntry(r *csv.Reader, key *[]byte) *Entry {
	// Should probably return a possible error here
	if r == nil {
		return &Entry{offset: Infinity}
	}
	fields, e := r.Read()
	if e == io.EOF {
		return &Entry{offset: Infinity}
	}
	return fieldsToMetadataEntry(fields, key)
}

// DataToHash returns the hash string for the data given
func DataToHash(buf []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(buf))
}

// Random Access Metadata
// This is used if there is a need for more random access to the
// metadata for snapshot metadata.

type MetadataDB struct {
	db []*string
}

func CreateDB(fileName string, key *[]byte) (*MetadataDB, error) {
	r, err := NewMetaReader(fileName)
	if err != nil {
		return nil, errors.New("CreateDB: " + err.Error())
	}
	// Do I need a reader/closer here to close the reader?

	mdb := &MetadataDB{}
	mdb.db = make([]*string, 1, 1000)

	// How do I make sure localDB is big enough?
	for mdep := NextEntry(r, key); mdep != nil; mdep = NextEntry(r, key) {
		index := mdep.offset >> objSizeBits
		if index >= OffsetT(cap(mdb.db)) {
			// resize localDB
			return nil, errors.New("Resize Needed")
		}

		mdb.db[index] = &mdep.ObjID
	}

	return mdb, nil
}

func (md *MetadataDB) OffsetToObject(offset OffsetT) (*string, error) {
	var index OffsetT = offset >> objSizeBits
	if index > OffsetT(len(md.db)) {
		return nil, errors.New("Offset Out of Range")
	}
	return md.db[index], nil
}
