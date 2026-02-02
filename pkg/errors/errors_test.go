package errors

import (
	"testing"
)

func TestErrorStrings(t *testing.T) {
	errs := []error{
		&TableAlreadyExistsError{Name: "users"},
		&TableNotFoundError{Name: "users"},
		&TwoPrimarykeysError{Total: 2},
		&PrimarykeyNotDefinedError{TableName: "users"},
		&DuplicateKeyError{Key: "10"},
		&IndexNotFoundError{Name: "email"},
	}

	for _, err := range errs {
		if err.Error() == "" {
			t.Errorf("Error string for %T should not be empty", err)
		}
	}
}
