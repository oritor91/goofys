// Copyright 2015 - 2017 Ka-Hing Cheung
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/jacobsa/fuse"
)

type FileHandle struct {
	inode *Inode

	mpuKey    *string
	dirty     bool
	writeInit sync.Once
	mpuWG     sync.WaitGroup
	etags     []*string

	mu              sync.Mutex
	mpuId           *string
	nextWriteOffset int64
	lastPartId      int

	poolHandle *BufferPool
	buf        *MBuf

	lastWriteError error

	// read
	cache *FileCache
	//metrics
	reporter *metricReporter
}

const MAX_READAHEAD = uint32(30 * 1024 * 1024)
const READAHEAD_CHUNK = uint32(5 * 1024 * 1024)

func NewFileHandle(in *Inode, reporter *metricReporter) *FileHandle {
	fh := &FileHandle{inode: in}
	fh.reporter = reporter
	return fh
}

func (fh *FileHandle) initWrite() {
	fh.writeInit.Do(func() {
		fh.mpuWG.Add(1)
		go fh.initMPU()
	})
}

func (fh *FileHandle) initMPU() {
	defer func() {
		fh.mpuWG.Done()
	}()

	fh.mpuKey = fh.inode.FullName()
	fs := fh.inode.fs
	ctx := context.Background()

	params := &s3.CreateMultipartUploadInput{
		Bucket:       &fs.bucket,
		Key:          fs.key(*fh.mpuKey),
		StorageClass: types.StorageClass(fs.flags.StorageClass),
		ContentType:  fs.getMimeType(*fh.inode.FullName()),
	}

	if fs.flags.UseSSE {
		params.ServerSideEncryption = fs.sseType
		if fs.flags.UseKMS && fs.flags.KMSKeyID != "" {
			params.SSEKMSKeyId = &fs.flags.KMSKeyID
		}
	}

	if fs.flags.ACL != "" {
		params.ACL = types.ObjectCannedACL(fs.flags.ACL)
	}

	if !fs.gcs {
		resp, err := fs.s3.CreateMultipartUpload(ctx, params)

		fh.mu.Lock()
		defer fh.mu.Unlock()

		if err != nil {
			fh.lastWriteError = mapAwsError(err)
			s3Log.Errorf("CreateMultipartUpload %v = %v", *fh.mpuKey, err)
			return
		}

		s3Log.Debug(resp)

		fh.mpuId = resp.UploadId
	} else {
		// GCS resumable upload: start a resumable session using a plain HTTP POST
		// The mpuId will hold the session URI returned in the Location header.
		endpointURL := ""
		if fs.awsConfig.BaseEndpoint != nil {
			endpointURL = *fs.awsConfig.BaseEndpoint
		}
		uploadURL := fmt.Sprintf("%s/%s/%s?uploads", endpointURL, fs.bucket, *fs.key(*fh.mpuKey))

		req, err := http.NewRequest("POST", uploadURL, bytes.NewReader([]byte{}))
		if err != nil {
			fh.mu.Lock()
			defer fh.mu.Unlock()
			fh.lastWriteError = mapAwsError(err)
			s3Log.Errorf("CreateMultipartUpload %v = %v", *fh.mpuKey, err)
			return
		}
		req.Header.Set("x-goog-resumable", "start")
		if params.ContentType != nil {
			req.Header.Set("Content-Type", *params.ContentType)
		}

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			fh.mu.Lock()
			defer fh.mu.Unlock()
			fh.lastWriteError = mapAwsError(err)
			s3Log.Errorf("CreateMultipartUpload GCS %v = %v", *fh.mpuKey, err)
			return
		}
		defer resp.Body.Close()

		location := resp.Header.Get("Location")
		_, err = url.Parse(location)
		if err != nil {
			fh.mu.Lock()
			defer fh.mu.Unlock()
			fh.lastWriteError = mapAwsError(err)
			s3Log.Errorf("CreateMultipartUpload %v = %v", *fh.mpuKey, err)
			return
		}

		fh.mu.Lock()
		defer fh.mu.Unlock()
		fh.mpuId = &location
	}

	fh.etags = make([]*string, 10000) // at most 10K parts

	return
}

func (fh *FileHandle) mpuPartNoSpawn(buf *MBuf, part int, total int64, last bool) (err error) {
	fs := fh.inode.fs
	ctx := context.Background()

	fs.replicators.Take(1, true)
	defer fs.replicators.Return(1)

	defer buf.Free()

	if part == 0 || part > 10000 {
		return errors.New(fmt.Sprintf("invalid part number: %v", part))
	}

	en := &fh.etags[part-1]

	if !fs.gcs {
		params := &s3.UploadPartInput{
			Bucket:     &fs.bucket,
			Key:        fs.key(*fh.inode.FullName()),
			PartNumber: aws.Int32(int32(part)),
			UploadId:   fh.mpuId,
			Body:       buf,
		}

		s3Log.Debug(params)

		resp, err := fs.s3.UploadPart(ctx, params)
		if err != nil {
			return mapAwsError(err)
		}

		if *en != nil {
			panic(fmt.Sprintf("etags for part %v already set: %v", part, **en))
		}
		*en = resp.ETag
	} else {
		// GCS resumable upload part: PUT to the session URI with Content-Range header
		bufSize := buf.Len()
		start := total - int64(bufSize)
		end := total - 1
		var size string
		if last {
			size = strconv.FormatInt(total, 10)
		} else {
			size = "*"
		}
		contentRange := fmt.Sprintf("bytes %v-%v/%v", start, end, size)

		req, reqErr := http.NewRequest("PUT", *fh.mpuId, buf)
		if reqErr != nil {
			return mapAwsError(reqErr)
		}
		req.Header.Set("Content-Length", strconv.Itoa(bufSize))
		req.Header.Set("Content-Range", contentRange)

		client := &http.Client{}
		resp, doErr := client.Do(req)
		if doErr != nil {
			return mapAwsError(doErr)
		}
		resp.Body.Close()

		if resp.StatusCode != 200 && resp.StatusCode != 201 && resp.StatusCode != 308 {
			return fmt.Errorf("GCS upload part returned status %v", resp.StatusCode)
		}
	}

	return
}

func (fh *FileHandle) mpuPart(buf *MBuf, part int, total int64) {
	defer func() {
		fh.mpuWG.Done()
	}()

	// maybe wait for CreateMultipartUpload
	if fh.mpuId == nil {
		fh.mpuWG.Wait()
		// initMPU might have errored
		if fh.mpuId == nil {
			return
		}
	}

	err := fh.mpuPartNoSpawn(buf, part, total, false)
	if err != nil {
		if fh.lastWriteError == nil {
			fh.lastWriteError = mapAwsError(err)
		}
	}
}

func (fh *FileHandle) waitForCreateMPU() (err error) {
	if fh.mpuId == nil {
		fh.mu.Unlock()
		fh.initWrite()
		fh.mpuWG.Wait() // wait for initMPU
		fh.mu.Lock()

		if fh.lastWriteError != nil {
			return fh.lastWriteError
		}
	}

	return
}

func (fh *FileHandle) partSize() uint64 {
	if fh.lastPartId < 1000 {
		return 5 * 1024 * 1024
	} else if fh.lastPartId < 2000 {
		return 25 * 1024 * 1024
	} else {
		return 125 * 1024 * 1024
	}
}

func (fh *FileHandle) uploadCurrentBuf(gcs bool) (err error) {
	err = fh.waitForCreateMPU()
	if err != nil {
		return
	}

	fh.lastPartId++
	part := fh.lastPartId
	buf := fh.buf
	fh.buf = nil

	if !gcs {
		fh.mpuWG.Add(1)
		go fh.mpuPart(buf, part, fh.nextWriteOffset)
	} else {
		// GCS doesn't support concurrent uploads
		err = fh.mpuPartNoSpawn(buf, part, fh.nextWriteOffset, false)
	}

	return
}

func (fh *FileHandle) WriteFile(offset int64, data []byte) (err error) {
	fh.inode.logFuse("WriteFile", offset, len(data))

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.lastWriteError != nil {
		return fh.lastWriteError
	}

	if offset != fh.nextWriteOffset {
		fh.inode.errFuse("WriteFile: only sequential writes supported", fh.nextWriteOffset, offset)
		fh.lastWriteError = syscall.ENOTSUP
		return fh.lastWriteError
	}

	fs := fh.inode.fs

	if offset == 0 {
		fh.poolHandle = fs.bufferPool
		fh.dirty = true
	}

	for {
		if fh.buf == nil {
			fh.buf = MBuf{}.Init(fh.poolHandle, fh.partSize(), true)
		}

		if fh.buf.Full() {
			err = fh.uploadCurrentBuf(fs.gcs)
			if err != nil {
				return
			}
			fh.buf = MBuf{}.Init(fh.poolHandle, fh.partSize(), true)
		}

		nCopied, _ := fh.buf.Write(data)
		fh.nextWriteOffset += int64(nCopied)

		if !fs.gcs {
			// don't upload a buffer post write for GCS
			// because we want to leave a buffer until
			// flush so that we can mark the last part
			// specially
			if fh.buf.Full() {
				err = fh.uploadCurrentBuf(fs.gcs)
				if err != nil {
					return
				}
			}
		}

		if nCopied == len(data) {
			break
		}

		data = data[nCopied:]
	}

	fh.inode.Attributes.Size = uint64(fh.nextWriteOffset)

	return
}

func (fh *FileHandle) ReadFile(offset int64, buf []byte) (bytesRead int, err error) {
	fh.inode.logFuse("ReadFile", offset, len(buf))
	defer func() {
		fh.inode.logFuse("< ReadFile", bytesRead, err)

		if err != nil {
			if err == io.EOF {
				err = nil
			}
		}
	}()

	fh.mu.Lock()
	defer fh.mu.Unlock()

	nwant := len(buf)
	var nread int

	if fh.cache == nil {
		fh.cache = NewFileCache(fh, fh.reporter)
	}

	for bytesRead < nwant && err == nil {
		nread, err = fh.readFile(offset+int64(bytesRead), buf[bytesRead:])
		if nread > 0 {
			bytesRead += nread
		}
	}

	return
}

func (fh *FileHandle) readFile(offset int64, buf []byte) (bytesRead int, err error) {
	defer func() {
		fh.inode.logFuse("< readFile", bytesRead, err)
	}()

	if uint64(offset) >= fh.inode.Attributes.Size {
		// nothing to read
		if fh.inode.Invalid {
			err = fuse.ENOENT
		} else if fh.inode.KnownSize == nil {
			err = io.EOF
		} else {
			err = io.EOF
		}
		return
	}

	fs := fh.inode.fs

	if fh.poolHandle == nil {
		fh.poolHandle = fs.bufferPool
	}

	read, err := fh.cache.Read(uint64(offset), buf)
	bytesRead = int(read)

	return
}

func (fh *FileHandle) Release() {
	// read buffers
	if fh.cache != nil {
		fh.cache.Release()
	}

	// write buffers
	if fh.poolHandle != nil {
		if fh.buf != nil && fh.buf.buffers != nil {
			if fh.lastWriteError == nil {
				panic("buf not freed but error is nil")
			}

			fh.buf.Free()
			// the other in-flight multipart PUT buffers will be
			// freed when they finish/error out
		}
	}

	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()

	if fh.inode.fileHandles == 0 {
		panic(fh.inode.fileHandles)
	}

	fh.inode.fileHandles -= 1
}

func (fh *FileHandle) flushSmallFile() (err error) {
	buf := fh.buf
	fh.buf = nil

	if buf == nil {
		buf = MBuf{}.Init(fh.poolHandle, 0, true)
	}

	defer buf.Free()

	fs := fh.inode.fs
	ctx := context.Background()

	storageClass := fs.flags.StorageClass
	if fh.nextWriteOffset < 128*1024 && storageClass == "STANDARD_IA" {
		storageClass = "STANDARD"
	}

	params := &s3.PutObjectInput{
		Bucket:       &fs.bucket,
		Key:          fs.key(*fh.inode.FullName()),
		Body:         buf,
		StorageClass: types.StorageClass(storageClass),
		ContentType:  fs.getMimeType(*fh.inode.FullName()),
	}

	if fs.flags.UseSSE {
		params.ServerSideEncryption = fs.sseType
		if fs.flags.UseKMS && fs.flags.KMSKeyID != "" {
			params.SSEKMSKeyId = &fs.flags.KMSKeyID
		}
	}

	if fs.flags.ACL != "" {
		params.ACL = types.ObjectCannedACL(fs.flags.ACL)
	}

	fs.replicators.Take(1, true)
	defer fs.replicators.Return(1)

	_, err = fs.s3.PutObject(ctx, params)
	if err != nil {
		err = mapAwsError(err)
		fh.lastWriteError = err
	}
	return
}

func (fh *FileHandle) resetToKnownSize() {
	if fh.inode.KnownSize != nil {
		fh.inode.Attributes.Size = *fh.inode.KnownSize
	} else {
		fh.inode.Attributes.Size = 0
		fh.inode.Invalid = true
	}
}

func (fh *FileHandle) FlushFile() (err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	fh.inode.logFuse("FlushFile")

	if !fh.dirty || fh.lastWriteError != nil {
		if fh.lastWriteError != nil {
			err = fh.lastWriteError
			fh.resetToKnownSize()
		}
		return
	}

	fs := fh.inode.fs
	ctx := context.Background()

	// abort mpu on error
	defer func() {
		if err != nil {
			if fh.mpuId != nil {
				go func() {
					params := &s3.AbortMultipartUploadInput{
						Bucket:   &fs.bucket,
						Key:      fs.key(*fh.inode.FullName()),
						UploadId: fh.mpuId,
					}

					fh.mpuId = nil
					resp, _ := fs.s3.AbortMultipartUpload(ctx, params)
					s3Log.Debug(resp)
				}()
			}

			fh.resetToKnownSize()
		} else {
			if fh.dirty {
				// don't unset this if we never actually flushed
				size := fh.inode.Attributes.Size
				fh.inode.KnownSize = &size
				fh.inode.Invalid = false
			}
			fh.dirty = false
		}

		fh.writeInit = sync.Once{}
		fh.nextWriteOffset = 0
		fh.lastPartId = 0
	}()

	if fh.lastPartId == 0 {
		return fh.flushSmallFile()
	}

	fh.mpuWG.Wait()

	if fh.lastWriteError != nil {
		return fh.lastWriteError
	}

	if fh.mpuId == nil {
		return
	}

	nParts := fh.lastPartId
	if fh.buf != nil {
		// upload last part
		nParts++
		err = fh.mpuPartNoSpawn(fh.buf, nParts, fh.nextWriteOffset, true)
		if err != nil {
			return
		}
	}

	if !fs.gcs {
		parts := make([]types.CompletedPart, nParts)
		for i := 0; i < nParts; i++ {
			parts[i] = types.CompletedPart{
				ETag:       fh.etags[i],
				PartNumber: aws.Int32(int32(i + 1)),
			}
		}

		params := &s3.CompleteMultipartUploadInput{
			Bucket:   &fs.bucket,
			Key:      fs.key(*fh.mpuKey),
			UploadId: fh.mpuId,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: parts,
			},
		}

		s3Log.Debug(params)

		resp, err := fs.s3.CompleteMultipartUpload(ctx, params)
		if err != nil {
			return mapAwsError(err)
		}

		s3Log.Debug(resp)
	} else {
		// nothing, we already uploaded last part
	}

	fh.mpuId = nil

	if *fh.mpuKey != *fh.inode.FullName() {
		// the file was renamed
		err = renameObject(ctx, fs, fh.nextWriteOffset, *fh.mpuKey, *fh.inode.FullName())
	}

	return
}
