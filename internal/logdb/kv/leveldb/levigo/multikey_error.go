package levigo

import (
	"fmt"
)

// MultiKeyError encapsulates multiple errors encountered in a Get/Put-Many() call,
// behind the errors.error interface. Caller may cast the generic error object to
// a MultiKeyError and call ErrorsByKeyIdx() to get all the errors by the failed key index.
type MultiKeyError struct {
	// invariant: errsByIdx is either nil or has at least ONE entry, it's never an empty map
	errsByIdx map[int]error
}

// Error ...
func (mke *MultiKeyError) Error() string {
	if mke.errsByIdx == nil {
		return ""
	}

	// Show a few errors to informed the user
	var topErrsMsg string
	const maxNumErrs = 3
	cnt := 0
	for i := range mke.errsByIdx {
		topErrsMsg += fmt.Sprintf(" keys[%d]:%s;", i, mke.errsByIdx[i].Error())
		cnt++
		if cnt >= maxNumErrs {
			break
		}
	}
	return fmt.Sprintf("%d keys encountered error (here's a few:%s), cast via err.(*levigo.MultiKeyErrors).ErrorsByKeyIdx() to get all the errors", len(mke.errsByIdx), topErrsMsg)
}

// GoString ...
func (mke *MultiKeyError) GoString() string {
	return fmt.Sprintf("*%#v", *mke)
}

// ErrorsByKeyIdx returns a result map of keyIdx => error. e.g.
// If one calls GetMany(keys) and keys[2], keys[3], keys[7]
// failed, then result[2] will hold the error for keys[2],
// result[3] for keys[3] and result[7] the error for keys[7]
func (mke *MultiKeyError) ErrorsByKeyIdx() map[int]error {
	if mke.errsByIdx == nil {
		return nil
	}

	result := make(map[int]error, len(mke.errsByIdx))
	for keyIdx, err := range mke.errsByIdx {
		result[keyIdx] = err
	}

	return result
}

func (mke *MultiKeyError) addKeyErr(keyIdx int, err error) {
	if mke.errsByIdx == nil {
		mke.errsByIdx = make(map[int]error)
	}
	// This means operation on keys[keyIdx] failed with err
	mke.errsByIdx[keyIdx] = err
}

func (mke *MultiKeyError) errAt(i int) error {
	if mke.errsByIdx == nil {
		return nil
	}
	err, ok := mke.errsByIdx[i]
	if !ok {
		return nil
	}
	return err
}
