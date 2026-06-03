package types

// NullKey represents an absent value (SQL NULL) used as a key. Ordering
// is NULLS FIRST: NULL is the minimum, sorting before every non-null
// value, and two NULLs are equal. This is consistent in both operand
// orders because every other key type treats NullKey as smaller than
// itself (see nullOrderingAgainst).
type NullKey struct{}

// Compare orders NullKey: equal to another NULL, less than anything else.
func (NullKey) Compare(other Comparable) (int, error) {
	if _, ok := other.(NullKey); ok {
		return 0, nil
	}
	return -1, nil
}

func (NullKey) String() string { return "NULL" }

// nullOrderingAgainst is the helper non-null key types call before their
// own type check: a non-null value always sorts after NULL, so when
// other is NullKey the comparison resolves to +1. The boolean reports
// whether other was NULL (and thus whether the returned int is final).
func nullOrderingAgainst(other Comparable) (int, bool) {
	if _, ok := other.(NullKey); ok {
		return 1, true
	}
	return 0, false
}
