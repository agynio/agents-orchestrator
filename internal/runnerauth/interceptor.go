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
	if strings.TrimSpace(secret) == "" {
		return nil, fmt.Errorf("DOCKER_RUNNER_SHARED_SECRET must be set")
	}
	return &signer{secret: []byte(secret)}, nil
}

func (s *signer) unaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	signedCtx, err := s.outgoingContext(ctx, method)
	if err != nil {
		return err
	}
	return invoker(signedCtx, method, req, reply, cc, opts...)
}

func (s *signer) streamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	signedCtx, err := s.outgoingContext(ctx, method)
	if err != nil {
		return nil, err
	}
	return streamer(signedCtx, desc, cc, method, opts...)
}

func (s *signer) outgoingContext(ctx context.Context, path string) (context.Context, error) {
	if path == "" {
		return nil, fmt.Errorf("runner auth requires method path")
	}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	nonce := uuid.NewString()
	payload := buildSignaturePayload(grpcMethod, path, timestamp, nonce, emptyBodyHash)
	signature, err := signPayload(s.secret, payload)
	if err != nil {
		return nil, err
	}
	return metadata.AppendToOutgoingContext(ctx,
		headerTimestamp, timestamp,
		headerNonce, nonce,
		headerSignature, signature,
	), nil
}

func hashBody(body string) string {
	h := sha256.Sum256([]byte(body))
	return base64.StdEncoding.EncodeToString(h[:])
}

func buildSignaturePayload(method, path, timestamp, nonce, bodyHash string) string {
	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s", strings.ToUpper(method), path, timestamp, nonce, bodyHash)
}

func signPayload(secret []byte, payload string) (string, error) {
	mac := hmac.New(sha256.New, secret)
	if _, err := mac.Write([]byte(payload)); err != nil {
		return "", fmt.Errorf("sign runner payload: %w", err)
	}
	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}
