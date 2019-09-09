package internal

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
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

func GetSize(fh *FileHandle) uint64 {
	return fh.inode.Attributes.Size
}

func offsetToIdx(offset uint64) uint32 {
	return uint32(offset / uint64(CacheChunkSize))
}

func idxToOffset(idx uint32) uint64 {
	return uint64(idx) * uint64(CacheChunkSize)
}

func (cache FileCache) readAheadOf(idx uint64) {
	maxChunk := GetSize(cache.fh) / uint64(CacheChunkSize)
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
		if buffers[i] != nil && !buffers[i].abort {
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
			fuseLog.Errorln(err)
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
	size   int
	buf    []byte
	fh     *FileHandle

	downloading bool
	read        int

	lock        sync.Mutex
	abort       bool
	downloadErr error
	readPending bool
}

func (b S3ReadBuffer) Init(fh *FileHandle, offset uint64, buf []byte) *S3ReadBuffer {
	fs := fh.inode.fs
	b.s3 = fs.s3
	b.read = 0
	b.size = len(buf)
	totalSize := GetSize(fh) - offset
	if uint64(b.size) > totalSize {
		b.size = int(totalSize) // cast to int is ok since totalSize is smaller than size which is int
	}
	b.offset = offset
	b.buf = buf
	b.fh = fh
	b.downloading = true
	go b.download()
	return &b
}

func (b *S3ReadBuffer) freeBuffer() []byte {
	res := b.buf
	b.abort = true
	for {
		b.lock.Lock()
		downloading := b.downloading
		b.lock.Unlock()
		if !downloading {
			break
		}
	}
	// after the loop, download has stopped
	b.buf = nil
	fuseLog.Debugln("Release buffer", b.fh.inode.Id, b.offset)
	return res
}

func (b *S3ReadBuffer) tryDownload() (err error, retry bool) {
	fs := b.fh.inode.fs
	params := &s3.GetObjectInput{
		Bucket: &fs.bucket,
		Key:    fs.key(*b.fh.inode.FullName()),
	}

	bytes := fmt.Sprintf("bytes=%v-%v", b.offset+uint64(b.read), b.offset+uint64(b.read+b.size)-1)
	params.Range = &bytes

	out, err := b.s3.GetObject(params)

	if err != nil {
		fmt.Printf("read: %d, range: %s\n", b.read, bytes)
		return err, false
	}
	defer out.Body.Close()
	for b.read < len(b.buf) && !b.abort {
		n, err := out.Body.Read(b.buf[b.read:])

		b.read += n

		if err != nil {
			if err == io.EOF {
				if b.read != b.size {
					fuseLog.Errorf("Unexpected EOF. read: %d, bytes: %s, wanted: %d", b.read, bytes, b.size)
				}
				return err, false
			}
			if strings.Contains(err.Error(), "SlowDown: Please reduce your request rate") {
				time.Sleep(time.Millisecond * time.Duration(100+rand.Int31n(3000)))
				return err, true
			}
			if strings.Contains(err.Error(), "Client.Timeout exceeded while reading body") {
				return err, true
			}
			return err, false
		}

		if !b.readPending {
			b.lock.Unlock()
			runtime.Gosched()
			b.lock.Lock()
		}
	}
	return nil, false
}

func (b *S3ReadBuffer) download() {
	b.lock.Lock()
	if b.read >= b.size {
		fuseLog.Debugf("already downloaded %d\n", offsetToIdx(b.offset))
		b.downloading = false
		b.lock.Unlock()
		return
	}
	fuseLog.Debugf("Starting download %d\n", offsetToIdx(b.offset))
	for i := 0; i < 3; i++ {
		b.downloadErr = nil
		err, retry := b.tryDownload()
		b.downloadErr = err
		if !retry {
			break
		}
		if err == nil {
			break
		}
	}
	fuseLog.Debugf("Done download: %s, buffer: %d, read: %d\n", b.downloadErr, offsetToIdx(b.offset), b.read)
	b.downloading = false
	b.lock.Unlock()
}

func (b *S3ReadBuffer) Read(offset int, p []byte) (n int, err error) {
	b.readPending = true
	wantToRead := len(p)
	if b.size-offset < wantToRead {
		fuseLog.Debugf("Changing want. old: %d, new %d\n", wantToRead, b.size)
		wantToRead = b.size - offset
	}

	var started bool

	for {
		b.lock.Lock()
		if wantToRead+offset <= b.read {
			b.lock.Unlock()
			break
		}
		if !b.downloading {
			if !started {
				fuseLog.Debugf("Restarting download %d\n", offsetToIdx(b.offset))
				started = true
				go b.download()
			} else {
				if wantToRead+offset <= b.read {
					b.lock.Unlock()
					break
				}
				err = b.downloadErr
				if err != nil && err != io.EOF {
					b.lock.Unlock()
					return 0, err
				}
				b.lock.Unlock()
				return 0, fmt.Errorf("%d download finished, but no error and not enough downloaded, read: %d, offset: %d, want: %d", offsetToIdx(b.offset), b.read, offset, wantToRead)
			}
		}
		b.lock.Unlock()
	}
	copied := copy(p, b.buf[offset:])
	return copied, nil
}
