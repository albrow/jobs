package zazu

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
)

func decode(reply []byte, dest interface{}) error {
	// Check the type of dest and make sure it is a pointer to something,
	// otherwise we can't set its value in any meaningful way.
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("zazu: Argument to decode must be pointer. Got %T", dest)
	}

	// Use the gob package to decode the reply and write the result into
	// dest.
	buf := bytes.NewBuffer(reply)
	dec := gob.NewDecoder(buf)
	if err := dec.DecodeValue(val.Elem()); err != nil {
		return err
	}
	return nil
}

func encode(data interface{}) ([]byte, error) {
	if data == nil {
		return nil, nil
	}
	buf := bytes.NewBuffer([]byte{})
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
