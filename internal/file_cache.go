package internal

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"container/list"

	"github.com/aws/aws-sdk-go/service/s3"
)

// Note these are overwritetn in any case by the flags
var RcMinChunks = uint32(64)
var RcMaxChunks = uint32(256)
var RcChunksPerFile = uint32(64)
var RcChunkSize = uint32(4 * 1024 * 1024)
var RcReadAheadCount = uint32(1)
var RcDownloadRetries = uint32(5)

var buffers map[string]*S3ReadBuffer
var lru *list.List

var maxChunks uint32
var desiredMaxChunks uint32

// MaxReadDuration Used by metric reporter as the threshold for slow reads
const MaxReadDuration = time.Millisecond * 100

func init() {
	maxChunks = RcMinChunks
	desiredMaxChunks = maxChunks
	buffers = make(map[string]*S3ReadBuffer)
	lru = list.New()
}

func changeDesiredMaxChunks(delta uint32) {
	desiredMaxChunks += delta
	maxChunks = MaxUInt32(MinUInt32(desiredMaxChunks, RcMaxChunks), RcMinChunks)
}

type FileCache struct {
	fh       *FileHandle
	reporter *metricReporter

	returnedToOs     uint64
	downloadedFromS3 uint64
}

func NewFileCache(fh *FileHandle, reporter *metricReporter) *FileCache {
	fc := FileCache{}
	fc.fh = fh
	fc.reporter = reporter

	changeDesiredMaxChunks(RcMaxChunks)
	return &fc
}

func releaseBuffer(buff *S3ReadBuffer) []byte {
	res := buff.freeBuffer()
	lru.Remove(buff.lruElem)
	delete(buffers, buff.idx.String())
	return res
}

func (cache *FileCache) Release() {
	changeDesiredMaxChunks(-RcChunksPerFile)
	for len(buffers) > int(maxChunks) {
		elem := lru.Back()
		if elem == nil {
			break
		}
		buff := elem.Value.(*S3ReadBuffer)
		releaseBuffer(buff)
	}
	template := "Closing file cache for %s. Total read from s3: %d, total returned to OS: %d"
	s3down := atomic.LoadUint64(&cache.downloadedFromS3)
	fuseLog.Infof(template, *cache.fh.inode.FullName(), s3down, cache.returnedToOs)
	fuseLog.Infof("Current maxChunks: %d, lru len: %d, map size %d", maxChunks, lru.Len(), len(buffers))
}

func GetSize(fh *FileHandle) uint64 {
	return fh.inode.Attributes.Size
}

func offsetToIdx(offset uint64) uint32 {
	return uint32(offset / uint64(RcChunkSize))
}

func idxToOffset(idx uint32) uint64 {
	return uint64(idx) * uint64(RcChunkSize)
}

func (cache *FileCache) readAheadOf(idx uint32) {
	maxChunk := uint32(GetSize(cache.fh) / uint64(RcChunkSize))
	start := idx + 1
	end := MinUInt32(maxChunk, idx+RcReadAheadCount)
	buffers := make([]*S3ReadBuffer, end-start+1)
	for i := start; i <= end; i++ {
		buffIdx := s3BufferIdx{cache, idxToOffset(i)}
		buffer, _, err := cache.getBuffer(buffIdx)
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

func (cache *FileCache) Read(offset uint64, buff []byte) (read uint64, err error) {
	start := time.Now()
	res, err := cache.read(offset, buff)
	end := time.Now()

	if cache.reporter != nil {
		go cache.reporter.Report(start, end)
	}

	atomic.AddUint64(&cache.returnedToOs, res)
	return res, err
}

func (cache *FileCache) read(offset uint64, buff []byte) (read uint64, err error) {

	expectToRead := uint64(len(buff))
	startBuff := uint32(offset / uint64(RcChunkSize))
	endBuff := uint32((offset + uint64(len(buff)) - 1) / uint64(RcChunkSize))
	buffers := make([]*S3ReadBuffer, endBuff-startBuff+1)
	fuseLog.Debugln("About to read ", cache.fh.inode.Id, startBuff, endBuff)

	for i := startBuff; i <= endBuff; i++ {
		buffIdx := s3BufferIdx{cache, idxToOffset(i)}
		buffer, hit, err := cache.getBuffer(buffIdx)
		var cacheHitTxt string
		if hit {
			cacheHitTxt = "hit  "
		} else {
			cacheHitTxt = "miss "
		}
		fuseLog.Debug("cache ", cacheHitTxt, buffer.idx)
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
		readTo := MinUInt64(uint64(RcChunkSize)-readOffset, uint64(len(buff)))
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

func (cache *FileCache) pushToLru(buffer *S3ReadBuffer) {
	idx := buffer.idx.String()
	_, ok := buffers[idx]
	if !ok {
		fuseLog.Debug("Added to lru ", cache.fh.inode.Id, idx, lru.Len())
		elem := lru.PushFront(buffer)
		buffer.lruElem = elem
		buffers[idx] = buffer
	} else {
		lru.MoveToFront(buffer.lruElem)
	}
}

func (cache *FileCache) getBuffer(idx s3BufferIdx) (buffer *S3ReadBuffer, hit bool, err error) {
	b, ok := buffers[idx.String()]
	if ok {
		return b, true, nil
	}

	var buf []byte
	fuseLog.Debug("Lru size ", cache.fh.inode.Id, lru.Len())
	if lru.Len() == int(maxChunks) {
		elem := lru.Back()
		b := elem.Value.(*S3ReadBuffer)
		buf = releaseBuffer(b)
	} else if lru.Len() > int(maxChunks) {
		panic("did not expect more chunks than allowed.")
	}

	if buf == nil {
		buf = make([]byte, RcChunkSize)
	}
	buffer = S3ReadBuffer{}.Init(cache, idx.offset, buf)
	fuseLog.Debug("Init buffer ", cache.fh.inode.Id, buffer.idx, buffer.offset, RcChunkSize)
	cache.pushToLru(buffer)
	return buffer, false, nil
}

type s3BufferIdx struct {
	fc     *FileCache
	offset uint64
}

func (idx s3BufferIdx) String() string {
	return fmt.Sprintf("%s;%d", *idx.fc.fh.inode.FullName(), offsetToIdx(idx.offset))
}

type S3ReadBuffer struct {
	idx     s3BufferIdx
	lruElem *list.Element

	s3     *s3.S3
	offset uint64
	size   int
	buf    []byte

	downloading bool
	read        int

	lock        sync.Mutex
	abort       bool
	downloadErr error
	readPending bool
}

func (b S3ReadBuffer) Init(fc *FileCache, offset uint64, buf []byte) *S3ReadBuffer {
	fs := fc.fh.inode.fs
	b.s3 = fs.s3
	b.read = 0
	b.size = len(buf)
	totalSize := GetSize(fc.fh) - offset
	if uint64(b.size) > totalSize {
		b.size = int(totalSize) // cast to int is ok since totalSize is smaller than size which is int
	}
	b.offset = offset
	b.buf = buf
	b.downloading = true

	b.idx.offset = offset
	b.idx.fc = fc

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
	fuseLog.Debugln("Release buffer", b.idx)
	return res
}

func (b *S3ReadBuffer) tryDownload() (err error, retry bool) {
	inode := b.idx.fc.fh.inode
	fs := inode.fs
	params := &s3.GetObjectInput{
		Bucket: &fs.bucket,
		Key:    fs.key(*inode.FullName()),
	}

	bytes := fmt.Sprintf("bytes=%v-%v", b.offset+uint64(b.read), b.offset+uint64(b.read+b.size)-1)
	params.Range = &bytes

	out, err := b.s3.GetObject(params)

	if err != nil {
		return err, true
	}
	defer out.Body.Close()
	for b.read < len(b.buf) && !b.abort {
		n, err := out.Body.Read(b.buf[b.read:])

		b.read += n
		atomic.AddUint64(&b.idx.fc.downloadedFromS3, uint64(n))

		if err != nil {
			if err == io.EOF {
				if b.read != b.size {
					fuseLog.Errorf("Unexpected EOF. read: %d, bytes: %s, wanted: %d", b.read, bytes, b.size)
				}
				return err, false
			}
			if strings.Contains(err.Error(), "SlowDown: Please reduce your request rate") {
				return err, true
			}
			if strings.Contains(err.Error(), "Client.Timeout exceeded while reading body") {
				return err, true
			}
			return err, true
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
	inode := b.idx.fc.fh.inode
	fs := inode.fs
	params := &s3.GetObjectInput{
		Bucket: &fs.bucket,
		Key:    fs.key(*inode.FullName()),
	}
	fuseLog.Infof("Starting download: %s/%s Idx: %d\n", *params.Bucket, *params.Key, offsetToIdx(b.offset))
	sleep_times := []int32{2000, 4000, 8000, 8000}
	for i := 0; i < int(RcDownloadRetries); i++ {
		b.downloadErr = nil
		err, retry := b.tryDownload()
		b.downloadErr = err
		if !retry || uint32(i+1) == RcDownloadRetries {
			break
		}
		if err == nil {
			break
		}
		// we want to sleep for 0-1, 0-2, 0-4, 0-8 accessing sleep_times according to the retry attempt
		// whould give us the desired result
		sleep_milliseconds := rand.Int31n(sleep_times[i]) + int32(100*(i+1)) // 100 * i for minimal delay between attempts
		fuseLog.Warnf("Got error: %s - Retry needed on attempt %d. Sleeping for %d seconds\n", err.Error(), i, sleep_milliseconds/1000)
		time.Sleep(time.Duration(sleep_milliseconds) * time.Millisecond)
	}
	fuseLog.Infof("Done download: %s, buffer: %d, read: %d\n", b.downloadErr, offsetToIdx(b.offset), b.read)
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
