package internal

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"time"

	"container/list"

	"github.com/aws/aws-sdk-go/service/s3"
)

const CacheChunkSize = uint32(4 * 1024 * 1024)
const MaxChunksForCache = int(256 * 1024 * 1024 / CacheChunkSize)
const ReadAheadCount = uint64(MaxChunksForCache / 4)
const MaxReadDuration = time.Millisecond * 100

type FileCache struct {
	buffers   map[uint32]*list.Element
	fh        *FileHandle
	lru       *list.List
	maxChunks int
	reporter  *metricReporter
}

func NewFileCache(fh *FileHandle, reporter *metricReporter) *FileCache {
	fc := FileCache{}
	fc.lru = list.New()
	fc.buffers = make(map[uint32]*list.Element)
	fc.fh = fh
	fc.maxChunks = MaxChunksForCache
	fc.reporter = reporter
	return &fc
}

func (cache FileCache) Release() {
	for cache.lru.Len() > 0 {
		elem := cache.lru.Back()
		cache.lru.Remove(elem)
		buff := elem.Value.(*S3ReadBuffer)
		buff.freeBuffer()
	}
}

func (cache FileCache) GetSize() uint64 {
	return cache.fh.inode.Attributes.Size
}

func offsetToIdx(offset uint64) uint32 {
	return uint32(offset / uint64(CacheChunkSize))
}

func idxToOffset(idx uint32) uint64 {
	return uint64(idx) * uint64(CacheChunkSize)
}

func (cache FileCache) readAheadOf(idx uint64) {
	maxChunk := cache.GetSize() / uint64(CacheChunkSize)
	start := idx + 1
	end := MinUInt64(maxChunk, idx+ReadAheadCount)
	buffers := make([]*S3ReadBuffer, end-start+1)
	for i := start; i <= end; i++ {
		buffer, err := cache.getBuffer(uint32(i))
		if err == nil {
			buffers[i-start] = buffer // if readahead failed, next time we might succeed
		}
	}
	for i := len(buffers) - 1; i >= 0; i-- {
		if buffers[i] != nil && !buffers[i].stopReading {
			cache.pushToLru(buffers[i])
		}
		if i == 0 {
			break
		}
	}
}

func (cache FileCache) Read(offset uint64, buff []byte) (read uint64, err error) {
	start := time.Now()
	res, err := cache.read(offset, buff)
	end := time.Now()
	if cache.reporter != nil {
		go cache.reporter.Report(start, end)
	}
	return res, err
}

func (cache FileCache) read(offset uint64, buff []byte) (read uint64, err error) {

	expectToRead := uint64(len(buff))
	startBuff := offset / uint64(CacheChunkSize)
	endBuff := (offset + uint64(len(buff)) - 1) / uint64(CacheChunkSize)
	buffers := make([]*S3ReadBuffer, endBuff-startBuff+1)
	fuseLog.Debugln("About to read ", cache.fh.inode.Id, startBuff, endBuff)

	for i := startBuff; i <= endBuff; i++ {
		buffer, err := cache.getBuffer(uint32(i))
		if err != nil {
			return 0, err
		}
		buffers[i-startBuff] = buffer
		cache.pushToLru(buffer)
	}

	nread := uint64(0)
	for i := startBuff; i <= endBuff; i++ {
		buffer := buffers[i-startBuff]
		readOffset := offset - buffer.offset
		readTo := MinUInt64(uint64(CacheChunkSize)-readOffset, uint64(len(buff)))
		fuseLog.Debugln("Read from buffer", cache.fh.inode.Id, readOffset, buffer.offset, readTo)
		read, err := buffer.Read(int(readOffset), buff[:readTo])
		if err != nil {
			return nread, err
		}
		cache.pushToLru(buffer)
		fuseLog.Debugln("Read returned", cache.fh.inode.Id, read)
		buff = buff[read:]
		offset += uint64(read)
		nread += uint64(read)
	}

	cache.readAheadOf(endBuff)

	fuseLog.Debugln("Done reading", cache.fh.inode.Id, nread)
	if nread != expectToRead {
		return nread, errors.New(fmt.Sprint("Read only ", nread, " expected: ", expectToRead))
	}
	return nread, nil
}

func (cache FileCache) pushToLru(buffer *S3ReadBuffer) {
	idx := offsetToIdx(buffer.offset)
	elem, ok := cache.buffers[idx]
	if !ok {
		fuseLog.Debug("Added to lru ", cache.fh.inode.Id, idx, cache.lru.Len())
		elem = cache.lru.PushFront(buffer)
		cache.buffers[idx] = elem
	} else {
		cache.lru.MoveToFront(elem)
	}
}

func (cache FileCache) getBuffer(idx uint32) (buffer *S3ReadBuffer, err error) {
	elem, ok := cache.buffers[idx]
	if ok {
		b := elem.Value.(*S3ReadBuffer)
		return b, nil
	}

	var buf []byte
	fuseLog.Debug("Lru size ", cache.fh.inode.Id, cache.lru.Len())
	if cache.lru.Len() == cache.maxChunks {
		elem = cache.lru.Back()
		b := elem.Value.(*S3ReadBuffer)

		cache.lru.Remove(elem)
		delete(cache.buffers, offsetToIdx(b.offset))
		buf = b.freeBuffer()
	} else if cache.lru.Len() > cache.maxChunks {
		panic("did not expect more chunks than allowed.")
	}

	if buf == nil {
		buf = make([]byte, CacheChunkSize)
	}
	fuseLog.Debug("Init buffer ", cache.fh.inode.Id, idx, idxToOffset(idx), CacheChunkSize)
	buffer = S3ReadBuffer{}.Init(cache.fh, idxToOffset(idx), buf)
	cache.pushToLru(buffer)
	return buffer, nil
}

type S3ReadBuffer struct {
	s3     *s3.S3
	offset uint64
	buf    []byte
	fh     *FileHandle
	read   int

	downloader  sync.Once
	stopReading bool
	downloadErr error
	readPending bool
}

func (b S3ReadBuffer) Init(fh *FileHandle, offset uint64, buf []byte) *S3ReadBuffer {
	fs := fh.inode.fs
	b.s3 = fs.s3
	b.offset = offset
	b.buf = buf
	b.fh = fh
	go b.downloader.Do(b.tryDownloading)
	return &b
}

func (b *S3ReadBuffer) freeBuffer() []byte {
	res := b.buf
	b.stopReading = true
	b.downloader.Do(b.tryDownloading)
	b.buf = nil
	fuseLog.Debugln("Release buffer", b.fh.inode.Id, b.offset)
	return res
}

func (b *S3ReadBuffer) tryDownloading() {
	b.download(3)
}

func (b *S3ReadBuffer) download(retryCount int) {
	if retryCount == 0 {
		b.fh.inode.errFuse("Download failed, no more retries", b.offset)
		return
	}
	fs := b.fh.inode.fs
	params := &s3.GetObjectInput{
		Bucket: &fs.bucket,
		Key:    fs.key(*b.fh.inode.FullName()),
	}

	bytes := fmt.Sprintf("bytes=%v-%v", b.offset, b.offset+uint64(len(b.buf))-1)
	params.Range = &bytes

	req, resp := fs.s3.GetObjectRequest(params)

	err := req.Send()
	b.downloadErr = err
	if err == nil {
		defer resp.Body.Close()
		b.read = 0
		readBuf := b.buf
		for b.read < len(b.buf) && !b.stopReading {
			n, err := resp.Body.Read(readBuf)

			if err != nil {
				if strings.Contains(err.Error(), "Client.Timeout exceeded while reading body") {
					fuseLog.Warnln("Timeout, retrying ", b.fh.inode.Id, b.offset)
					b.download(retryCount - 1)
					return
				}
				b.downloadErr = err
				break
			}

			b.read += n
			readBuf = readBuf[n:]
			if !b.readPending {
				runtime.Gosched()
			}
		}
	}

	fuseLog.Debugln("Download finished", b.fh.inode.Id, b.offset, len(b.buf))
}

func (b *S3ReadBuffer) Read(offset int, p []byte) (n int, err error) {
	b.readPending = true
	if offset+len(p) > b.read {
		b.downloader.Do(b.tryDownloading)
	}
	if b.downloadErr != nil && b.downloadErr != io.EOF {
		b.fh.inode.errFuse("Error download", b.downloadErr)
		return 0, b.downloadErr
	}
	copied := copy(p, b.buf[offset:])
	return copied, nil
}
