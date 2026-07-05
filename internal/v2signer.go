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
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

var (
	errInvalidMethod = errors.New("v2 signer does not handle HTTP POST")
)

const (
	signatureVersion = "2"
	signatureMethod  = "HmacSHA1"
	timeFormat       = "Mon, 2 Jan 2006 15:04:05 +0000"
)

var subresources = []string{
	"acl",
	"delete",
	"lifecycle",
	"location",
	"logging",
	"notification",
	"partNumber",
	"policy",
	"requestPayment",
	"torrent",
	"uploadId",
	"uploads",
	"versionId",
	"versioning",
	"versions",
	"website",
}

type signer struct {
	Request   *http.Request
	Time      time.Time
	AccessKey string
	SecretKey string
	Token     string
	pathStyle bool

	Query        url.Values
	stringToSign string
	signature    string
}

// makeSignV2Middleware returns a smithy FinalizeMiddleware that signs requests with AWS Signature Version 2.
// This is used for S3-compatible endpoints that don't support SigV4.
func makeSignV2Middleware(cfg *aws.Config) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc(
			"goofys/SignV2",
			func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
				req, ok := in.Request.(*smithyhttp.Request)
				if !ok {
					return next.HandleFinalize(ctx, in)
				}

				if cfg.Credentials == nil {
					return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("v2signer: no credentials provider configured")
				}
				creds, err := cfg.Credentials.Retrieve(ctx)
				if err != nil {
					return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("v2signer: retrieve credentials: %w", err)
				}

				// skip signing for anonymous credentials
				if creds.AccessKeyID == "" && creds.SecretAccessKey == "" {
					return next.HandleFinalize(ctx, in)
				}

				v2 := signer{
					Request:   req.Request,
					Time:      time.Now().UTC(),
					AccessKey: creds.AccessKeyID,
					SecretKey: creds.SecretAccessKey,
					Token:     creds.SessionToken,
					pathStyle: true,
				}

				if err := v2.Sign(); err != nil {
					return middleware.FinalizeOutput{}, middleware.Metadata{}, err
				}

				return next.HandleFinalize(ctx, in)
			},
		), middleware.Before)
	}
}

func (v2 *signer) Sign() error {
	v2.Query = v2.Request.URL.Query()

	contentMD5 := v2.Request.Header.Get("Content-MD5")
	contentType := v2.Request.Header.Get("Content-Type")
	date := v2.Time.Format(timeFormat)
	v2.Request.Header.Set("x-amz-date", date)

	if v2.Token != "" {
		v2.Request.Header.Set("x-amz-security-token", v2.Token)
	}

	// in case this is a retry, ensure no signature present
	v2.Request.Header.Del("Authorization")

	method := v2.Request.Method

	uri := v2.Request.URL.Opaque
	if uri != "" {
		if questionMark := strings.Index(uri, "?"); questionMark != -1 {
			uri = uri[0:questionMark]
		}
		uri = "/" + strings.Join(strings.Split(uri, "/")[3:], "/")
	} else {
		uri = v2.Request.URL.Path
	}
	path := pathEscape(uri)
	if !v2.pathStyle {
		host := strings.SplitN(v2.Request.URL.Host, ".", 2)[0]
		path = "/" + host + uri
	}
	if path == "" {
		path = "/"
	}

	// build URL-encoded query keys and values
	queryKeysAndValues := []string{}
	for _, key := range subresources {
		if _, ok := v2.Query[key]; ok {
			k := strings.Replace(url.QueryEscape(key), "+", "%20", -1)
			v := strings.Replace(url.QueryEscape(v2.Query.Get(key)), "+", "%20", -1)
			if v != "" {
				v = "=" + v
			}
			queryKeysAndValues = append(queryKeysAndValues, k+v)
		}
	}

	// join into one query string
	query := strings.Join(queryKeysAndValues, "&")

	if query != "" {
		path += "?" + query
	}

	tmp := []string{
		method,
		contentMD5,
		contentType,
		"",
	}

	var headers []string
	for k := range v2.Request.Header {
		k = strings.ToLower(k)
		if strings.HasPrefix(k, "x-amz-") {
			headers = append(headers, k)
		}
	}
	sort.Strings(headers)

	for _, k := range headers {
		v := strings.Join(v2.Request.Header[http.CanonicalHeaderKey(k)], ",")
		tmp = append(tmp, k+":"+v)
	}

	tmp = append(tmp, path)

	// build the canonical string for the V2 signature
	v2.stringToSign = strings.Join(tmp, "\n")

	hash := hmac.New(sha1.New, []byte(v2.SecretKey))
	hash.Write([]byte(v2.stringToSign))
	v2.signature = base64.StdEncoding.EncodeToString(hash.Sum(nil))
	v2.Request.Header.Set("Authorization",
		"AWS "+v2.AccessKey+":"+v2.signature)

	return nil
}
