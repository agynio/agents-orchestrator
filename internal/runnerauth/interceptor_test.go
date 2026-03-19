package runnerauth

import "testing"

func TestHashBodyEmpty(t *testing.T) {
	expected := "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
	if got := hashBody(""); got != expected {
		t.Fatalf("expected empty body hash %q, got %q", expected, got)
	}
}

func TestBuildSignaturePayloadAndSign(t *testing.T) {
	secret := "test-secret"
	method := "post"
	path := "/agynio.runner.v1.RunnerService/FindWorkloadsByLabels"
	timestamp := "1700000000000"
	nonce := "6f9a2b7d-9b45-4bba-8e10-8e7cce8072b6"
	bodyHash := "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="

	expectedPayload := "POST\n" + path + "\n" + timestamp + "\n" + nonce + "\n" + bodyHash
	payload := buildSignaturePayload(method, path, timestamp, nonce, bodyHash)
	if payload != expectedPayload {
		t.Fatalf("expected payload %q, got %q", expectedPayload, payload)
	}

	expectedSignature := "4ETIVMCQlIaQtqa4IRwOmcVnHlpUowqYgfWOnRdCiBU="
	signature := signPayload([]byte(secret), payload)
	if signature != expectedSignature {
		t.Fatalf("expected signature %q, got %q", expectedSignature, signature)
	}
}

func TestNewSignerTrimsSecret(t *testing.T) {
	signer, err := newSigner("  secret  ")
	if err != nil {
		t.Fatalf("newSigner: %v", err)
	}
	if string(signer.secret) != "secret" {
		t.Fatalf("expected trimmed secret %q, got %q", "secret", signer.secret)
	}
}

func TestNewSignerRejectsEmptySecret(t *testing.T) {
	if _, err := newSigner(" \t "); err == nil {
		t.Fatal("expected error for empty secret")
	}
}
