package errors

import "testing"

func TestErrors_ErrorMethod(t *testing.T) {
	errs := []error{
		&TableAlreadyExistsError{Name: "t1"},
		&TableNotFoundError{Name: "t1"},
		&TwoPrimarykeysError{Total: 2},
		&PrimarykeyNotDefinedError{TableName: "t1"},
		&DuplicateKeyError{Key: "k1"},
		&IndexNotFoundError{Name: "i1"},
		&InvalidKeyTypeError{Name: "i1", TypeName: "int"},
	}

	for _, e := range errs {
		if e.Error() == "" {
			t.Errorf("Error() returned empty string for %T", e)
		}
	}
}
