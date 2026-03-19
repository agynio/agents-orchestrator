package runnerauth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	headerTimestamp = "x-dr-timestamp"
	headerNonce     = "x-dr-nonce"
	headerSignature = "x-dr-signature"
	grpcMethod      = "POST"
)

var emptyBodyHash = hashBody("")

func NewClientInterceptors(secret string) (grpc.UnaryClientInterceptor, grpc.StreamClientInterceptor, error) {
	signer, err := newSigner(secret)
	if err != nil {
		return nil, nil, err
	}
	return signer.unaryInterceptor, signer.streamInterceptor, nil
}

type signer struct {
	secret []byte
}

func newSigner(secret string) (*signer, error) {
	trimmed := strings.TrimSpace(secret)
	if trimmed == "" {
		return nil, fmt.Errorf("runner auth: shared secret must not be empty")
	}
	return &signer{secret: []byte(trimmed)}, nil
}

func (s *signer) unaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(s.outgoingContext(ctx, method), method, req, reply, cc, opts...)
}

func (s *signer) streamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return streamer(s.outgoingContext(ctx, method), desc, cc, method, opts...)
}

func (s *signer) outgoingContext(ctx context.Context, path string) context.Context {
	if path == "" {
		panic("runner auth requires method path")
	}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	nonce := uuid.NewString()
	payload := buildSignaturePayload(grpcMethod, path, timestamp, nonce, emptyBodyHash)
	signature := signPayload(s.secret, payload)
	return metadata.AppendToOutgoingContext(ctx,
		headerTimestamp, timestamp,
		headerNonce, nonce,
		headerSignature, signature,
	)
}

func hashBody(body string) string {
	h := sha256.Sum256([]byte(body))
	return base64.StdEncoding.EncodeToString(h[:])
}

func buildSignaturePayload(method, path, timestamp, nonce, bodyHash string) string {
	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s", strings.ToUpper(method), path, timestamp, nonce, bodyHash)
}

func signPayload(secret []byte, payload string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(payload))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
