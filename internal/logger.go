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
	"context"
	"fmt"
	glog "log"
	"log/syslog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/smithy-go/logging"
	"github.com/sirupsen/logrus"
	logrus_syslog "github.com/sirupsen/logrus/hooks/syslog"
)

var mu sync.Mutex
var loggers = make(map[string]*LogHandle)

var log = GetLogger("main")
var fuseLog = GetLogger("fuse")

var syslogHook *logrus_syslog.SyslogHook

func InitLoggers(logToSyslog bool, cwRegion, cwGroup, cwName string) {
	if cwRegion != "" && cwGroup != "" && cwName != "" {
		hook, err := newCloudWatchLogsHook(cwRegion, cwGroup, cwName)
		if err != nil {
			log.Println(fmt.Sprintf("Could not create cloudwatch log: %s", err.Error()))
		} else {
			mu.Lock()
			for _, l := range loggers {
				l.Hooks.Add(hook)
			}
			mu.Unlock()
		}
	}

	if logToSyslog {
		hook, err := logrus_syslog.NewSyslogHook("", "", syslog.LOG_DEBUG, "")
		if err != nil {
			log.Println(fmt.Sprintf("Unable to connect to local syslog daemon: %s", err.Error()))
		} else {
			mu.Lock()
			for _, l := range loggers {
				l.Hooks.Add(hook)
			}
			mu.Unlock()
		}
	}
}

type LogHandle struct {
	logrus.Logger

	name string
	Lvl  *logrus.Level
}

func (l *LogHandle) Format(e *logrus.Entry) ([]byte, error) {
	// Mon Jan 2 15:04:05 -0700 MST 2006
	timestamp := ""
	lvl := e.Level
	if l.Lvl != nil {
		lvl = *l.Lvl
	}

	if syslogHook == nil {
		const timeFormat = "2006/01/02 15:04:05.000000"

		timestamp = e.Time.Format(timeFormat) + " "
	}

	str := fmt.Sprintf("%v%v.%v %v",
		timestamp,
		l.name,
		strings.ToUpper(lvl.String()),
		e.Message)

	if len(e.Data) != 0 {
		str += " " + fmt.Sprint(e.Data)
	}

	str += "\n"
	return []byte(str), nil
}

// Log implements aws.Logger (v1 compat, not needed for v2 but harmless)
func (l *LogHandle) Log(args ...interface{}) {
	l.Debugln(args...)
}

// Logf implements logging.Logger for AWS SDK v2
func (l *LogHandle) Logf(classification logging.Classification, format string, v ...interface{}) {
	l.Debugf(format, v...)
}

func NewLogger(name string) *LogHandle {
	l := &LogHandle{name: name}
	l.Out = os.Stderr
	l.Formatter = l
	l.Level = logrus.InfoLevel
	l.Hooks = make(logrus.LevelHooks)
	if syslogHook != nil {
		l.Hooks.Add(syslogHook)
	}
	return l
}

func GetLogger(name string) *LogHandle {
	mu.Lock()
	defer mu.Unlock()

	if logger, ok := loggers[name]; ok {
		return logger
	} else {
		logger := NewLogger(name)
		loggers[name] = logger
		return logger
	}
}

func GetStdLogger(l *LogHandle, lvl logrus.Level) *glog.Logger {
	mu.Lock()
	defer mu.Unlock()

	w := l.Writer()
	l.Formatter.(*LogHandle).Lvl = &lvl
	l.Level = lvl
	return glog.New(w, "", 0)
}

// cloudWatchLogsHook is an internal logrus hook that ships log lines to CloudWatch Logs.
type cloudWatchLogsHook struct {
	svc               *cloudwatchlogs.Client
	groupName         string
	streamName        string
	nextSequenceToken *string
	m                 sync.Mutex
	ch                chan *cwltypes.InputLogEvent
}

func newCloudWatchLogsHook(region, groupName, streamName string) (*cloudWatchLogsHook, error) {
	cfg := aws.Config{Region: region}
	svc := cloudwatchlogs.NewFromConfig(cfg)
	h := &cloudWatchLogsHook{
		svc:        svc,
		groupName:  groupName,
		streamName: streamName,
		ch:         make(chan *cwltypes.InputLogEvent, 10000),
	}

	// ensure log group + stream exist
	if err := h.ensureGroupAndStream(); err != nil {
		return nil, err
	}

	go h.putBatches(time.NewTicker(5 * time.Second).C)
	return h, nil
}

func (h *cloudWatchLogsHook) ensureGroupAndStream() error {
	ctx := context.Background()

	resp, err := h.svc.DescribeLogStreams(ctx, &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String(h.groupName),
		LogStreamNamePrefix: aws.String(h.streamName),
	})
	if err != nil {
		// try creating the group first
		_, cerr := h.svc.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
			LogGroupName: aws.String(h.groupName),
		})
		if cerr != nil {
			return cerr
		}
		resp, err = h.svc.DescribeLogStreams(ctx, &cloudwatchlogs.DescribeLogStreamsInput{
			LogGroupName:        aws.String(h.groupName),
			LogStreamNamePrefix: aws.String(h.streamName),
		})
		if err != nil {
			return err
		}
	}

	if len(resp.LogStreams) > 0 {
		h.nextSequenceToken = resp.LogStreams[0].UploadSequenceToken
		return nil
	}

	_, err = h.svc.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(h.groupName),
		LogStreamName: aws.String(h.streamName),
	})
	return err
}

func (h *cloudWatchLogsHook) putBatches(ticker <-chan time.Time) {
	var batch []*cwltypes.InputLogEvent
	size := 0
	for {
		select {
		case p := <-h.ch:
			messageSize := len(*p.Message) + 26
			if size+messageSize >= 1048576 || len(batch) == 10000 {
				go h.sendBatch(batch)
				batch = nil
				size = 0
			}
			batch = append(batch, p)
			size += messageSize
		case <-ticker:
			go h.sendBatch(batch)
			batch = nil
			size = 0
		}
	}
}

func (h *cloudWatchLogsHook) sendBatch(batch []*cwltypes.InputLogEvent) {
	h.m.Lock()
	defer h.m.Unlock()

	if len(batch) == 0 {
		return
	}

	events := make([]cwltypes.InputLogEvent, len(batch))
	for i, e := range batch {
		events[i] = *e
	}

	params := &cloudwatchlogs.PutLogEventsInput{
		LogEvents:     events,
		LogGroupName:  aws.String(h.groupName),
		LogStreamName: aws.String(h.streamName),
		SequenceToken: h.nextSequenceToken,
	}
	resp, err := h.svc.PutLogEvents(context.Background(), params)
	if err == nil {
		h.nextSequenceToken = resp.NextSequenceToken
	}
}

func (h *cloudWatchLogsHook) Fire(entry *logrus.Entry) error {
	line, err := entry.String()
	if err != nil {
		return err
	}
	ts := entry.Time.UnixNano() / int64(time.Millisecond)
	h.ch <- &cwltypes.InputLogEvent{
		Message:   aws.String(line),
		Timestamp: aws.Int64(ts),
	}
	return nil
}

func (h *cloudWatchLogsHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}
