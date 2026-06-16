// Copyright 2016 Ka-Hing Cheung
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

	. "gopkg.in/check.v1"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type MinioTest struct {
	fs *Goofys
}

var _ = Suite(&MinioTest{})

func (s *MinioTest) SetUpSuite(t *C) {
	endpoint := "https://play.minio.io:9000"
	awsConfig := aws.Config{
		Credentials: credentials.NewStaticCredentialsProvider("Q3AM3UQ867SPQQA43P2F",
			"zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG", ""),
		Region:      "us-east-1",
		BaseEndpoint: &endpoint,
	}

	s.fs = &Goofys{
		awsConfig: &awsConfig,
	}

	s.fs.s3 = s.fs.newS3()
	_, err := s.fs.s3.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	t.Assert(err, IsNil)
}

func (s *MinioTest) SetUpTest(t *C) {
	bucket := RandStringBytesMaskImprSrc(32)

	_, err := s.fs.s3.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	t.Assert(err, IsNil)

	uid, gid := MyUserAndGroup()
	flags := &FlagStorage{
		StorageClass: "STANDARD",
		DirMode:      0700,
		FileMode:     0700,
		Uid:          uint32(uid),
		Gid:          uint32(gid),
	}

	s.fs = NewGoofys(context.Background(), bucket, s.fs.awsConfig, flags)
	t.Assert(s.fs, NotNil)
}

func (s *MinioTest) TestNoop(t *C) {
}

// suppress unused import
var _ = types.StorageClass("")
