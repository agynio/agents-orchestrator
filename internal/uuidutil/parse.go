package uuidutil

import (
	"fmt"

	"github.com/google/uuid"
)

func ParseUUID(value, field string) (uuid.UUID, error) {
	if value == "" {
		return uuid.UUID{}, fmt.Errorf("%s is empty", field)
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("parse %s: %w", field, err)
	}
	return parsed, nil
}
