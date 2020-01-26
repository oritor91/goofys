package internal

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

type metricReporter struct {
	readDurations  []time.Duration
	lastUpdateTime time.Time
	cw             *cloudwatch.CloudWatch
	cwMetric       string
	cwId           string
}

func NewMetricReporter(sess *session.Session, cwMetric, cwId string) *metricReporter {
	res := &metricReporter{}
	res.lastUpdateTime = time.Now()
	res.cwId = cwId
	res.cwMetric = cwMetric

	res.cw = cloudwatch.New(sess)
	return res
}

func (reporter *metricReporter) Report(start time.Time, end time.Time) {
	duration := end.Sub(start)
	reporter.readDurations = append(reporter.readDurations, duration)

	if duration > MaxReadDuration {
		s3Log.Warnln("Read took longer than expected", duration)
	}
	timeSinceLastUpdate := end.Sub(reporter.lastUpdateTime)
	if timeSinceLastUpdate.Seconds() < 30 {
		return
	}

	reporter.lastUpdateTime = end
	sum := float64(0)
	durCnt := len(reporter.readDurations)
	for i := 0; i < durCnt; i++ {
		sum += reporter.readDurations[i].Seconds()
	}
	sumMs := sum * 1000
	avg := sumMs / float64(durCnt)

	reporter.readDurations = reporter.readDurations[:0]

	cw := reporter.cw
	_, err := cw.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String(reporter.cwMetric),
		MetricData: []*cloudwatch.MetricDatum{
			&cloudwatch.MetricDatum{
				MetricName: aws.String("ReadCallDuration"),
				Unit:       aws.String("Milliseconds"),
				Value:      aws.Float64(avg),
				Dimensions: []*cloudwatch.Dimension{
					&cloudwatch.Dimension{
						Name:  aws.String("By run-name/clip"),
						Value: aws.String(reporter.cwId),
					},
				},
			},
		},
	})
	if err != nil {
		fuseLog.Warn("Could not put metric", err)
	}
}
