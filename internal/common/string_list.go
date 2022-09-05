package common

type StringList []string

func (l StringList) Clone() StringList {
	clone := make([]string, 0, len(l))
	for _, s := range l {
		clone = append(clone, s)
	}
	return clone
}

func (l StringList) Equal(other StringList) bool {
	if len(l) != len(other) {
		return false
	}
	for i := range l {
		if l[i] != other[i] {
			return false
		}
	}
	return true
}

func CloneStringList(l StringList) StringList {
	return l.Clone()
}
