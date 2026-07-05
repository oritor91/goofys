package internal

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// noopFinalizeHandler is a FinalizeHandler that does nothing.
type noopFinalizeHandler struct{}

func (noopFinalizeHandler) Handle(_ context.Context, _ interface{}) (interface{}, middleware.Metadata, error) {
	return nil, middleware.Metadata{}, nil
}

func buildSmithyRequest(t *testing.T, method, rawURL string) *smithyhttp.Request {
	t.Helper()
	r, err := http.NewRequest(method, rawURL, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	return &smithyhttp.Request{Request: r}
}

// invokeV2Signer applies makeSignV2Middleware to a real smithy Stack, then
// invokes only the Finalize step directly so the request is not cloned by
// other stack steps.
func invokeV2Signer(t *testing.T, cfg *aws.Config, sr *smithyhttp.Request) error {
	t.Helper()
	stack := middleware.NewStack("test", smithyhttp.NewStackRequest)
	if err := makeSignV2Middleware(cfg)(stack); err != nil {
		t.Fatalf("makeSignV2Middleware stack registration: %v", err)
	}
	_, _, err := stack.Finalize.HandleMiddleware(context.Background(), sr, noopFinalizeHandler{})
	return err
}

// TestV2SignerNilCredentials verifies that nil cfg.Credentials returns an error
// instead of panicking — regression test for the production nil-pointer
// dereference reported after "Falling back to v2 signer".
func TestV2SignerNilCredentials(t *testing.T) {
	cfg := &aws.Config{} // Credentials is nil
	sr := buildSmithyRequest(t, "GET", "https://s3.amazonaws.com/bucket/key")
	err := invokeV2Signer(t, cfg, sr)
	if err == nil {
		t.Fatal("expected error for nil credentials, got nil")
	}
	if !strings.Contains(err.Error(), "no credentials provider configured") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

// TestV2SignerSetsAuthorizationHeader verifies that a signed request carries an
// Authorization header in AWS v2 format (AWS <key>:<signature>).
func TestV2SignerSetsAuthorizationHeader(t *testing.T) {
	cfg := &aws.Config{
		Credentials: credentials.NewStaticCredentialsProvider(
			"AKIAIOSFODNN7EXAMPLE",
			"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			"",
		),
	}
	sr := buildSmithyRequest(t, "GET", "https://s3.amazonaws.com/bucket/key")
	if err := invokeV2Signer(t, cfg, sr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	auth := sr.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS AKIAIOSFODNN7EXAMPLE:") {
		t.Fatalf("unexpected Authorization header: %q", auth)
	}
}
