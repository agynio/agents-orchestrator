package reconciler

import (
	"testing"

	"github.com/google/uuid"
)

func TestNormalizeRunnerWorkloadID(t *testing.T) {
	validID := uuid.New().String()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "prefixed-uuid",
			input: "workload-" + validID,
			want:  validID,
		},
		{
			name:  "uuid",
			input: validID,
			want:  validID,
		},
		{
			name:  "prefixed-non-uuid",
			input: "workload-not-a-uuid",
			want:  "workload-not-a-uuid",
		},
		{
			name:  "prefix-only",
			input: "workload-",
			want:  "workload-",
		},
		{
			name:  "empty",
			input: "",
			want:  "",
		},
		{
			name:  "other-prefix",
			input: "task-" + validID,
			want:  "task-" + validID,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := normalizeRunnerWorkloadID(test.input)
			if got != test.want {
				t.Fatalf("expected %q, got %q", test.want, got)
			}
		})
	}
}
