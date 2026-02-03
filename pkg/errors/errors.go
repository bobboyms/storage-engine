package errors

import (
	"fmt"
)

type TableAlreadyExistsError struct {
	Name string
}

func (e *TableAlreadyExistsError) Error() string {
	return fmt.Sprintf("table %q already exists", e.Name)
}

type TableNotFoundError struct {
	Name string
}

func (e *TableNotFoundError) Error() string {
	return fmt.Sprintf("table %q not found", e.Name)
}

type TwoPrimarykeysError struct {
	Total int
}

func (e *TwoPrimarykeysError) Error() string {
	return fmt.Sprintf("You have defined a total of %q primary keys. Only one primary key is allowed.", e.Total)
}

type PrimarykeyNotDefinedError struct {
	TableName string
}

func (e *PrimarykeyNotDefinedError) Error() string {
	return fmt.Sprintf("Primary key not defined. Table name: %q", e.TableName)
}

type DuplicateKeyError struct {
	Key string
}

func (e *DuplicateKeyError) Error() string {
	return fmt.Sprintf("duplicate key violation: key %q already exists in unique index", e.Key)
}

type IndexNotFoundError struct {
	Name string
}

func (e *IndexNotFoundError) Error() string {
	return fmt.Sprintf("index %q not found", e.Name)
}

type InvalidKeyTypeError struct {
	Name     string
	TypeName string
}

func (e *InvalidKeyTypeError) Error() string {
	return fmt.Sprintf("invalid key type for index %q: %s", e.Name, e.TypeName)
}
