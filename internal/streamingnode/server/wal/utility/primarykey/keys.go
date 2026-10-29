package primarykey

// Kind tells which column of Keys carries values.
type Kind uint8

const (
	KindNone Kind = iota
	KindInt64
	KindString
	KindMixed
)

// Keys is a columnar batch of primary keys taken from one or more WAL messages.
// Kind must describe which of Int64Values and StringValues is populated.
// Both slices may alias the slices of the decoded message body.
type Keys struct {
	Kind         Kind
	Int64Values  []int64
	StringValues []string
}

func (k Keys) Len() int {
	return len(k.Int64Values) + len(k.StringValues)
}

func (k Keys) Clone() Keys {
	return Keys{
		Kind:         k.Kind,
		Int64Values:  append([]int64(nil), k.Int64Values...),
		StringValues: append([]string(nil), k.StringValues...),
	}
}

// ToAny returns the int64 values followed by the string values.
func (k Keys) ToAny() []any {
	values := make([]any, 0, k.Len())
	for _, value := range k.Int64Values {
		values = append(values, value)
	}
	for _, value := range k.StringValues {
		values = append(values, value)
	}
	return values
}

// Append adds other to k. Kind becomes KindMixed once both columns are used.
func (k *Keys) Append(other Keys) {
	if other.Len() == 0 {
		return
	}
	if k.Kind == KindNone {
		k.Kind = other.Kind
	}
	if k.Kind != other.Kind && k.Kind != KindMixed {
		k.Kind = KindMixed
	}
	k.Int64Values = append(k.Int64Values, other.Int64Values...)
	k.StringValues = append(k.StringValues, other.StringValues...)
}
