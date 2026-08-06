// Package satoshi provides a signed satoshi value type with overflow-checked
// arithmetic bounded by the Bitcoin supply. It is the value type the funder's
// fee and change math is written in; negative values are legal (e.g. a funding
// target after subtracting provided inputs from provided outputs).
package satoshi

import (
	"fmt"
	"iter"
	"reflect"

	"github.com/go-softwarelab/common/pkg/types"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// Value is a signed satoshi amount. It may be negative during intermediate
// arithmetic; conversions to uint64 reject negatives.
type Value int64

// Int64 returns the value as a plain int64.
func (v Value) Int64() int64 {
	return int64(v)
}

// UInt64 returns the value as a uint64, or an error when it is negative.
func (v Value) UInt64() (uint64, error) {
	if v < 0 {
		return 0, fmt.Errorf("cannot convert negative satoshi to uint64")
	}
	return uint64(v), nil
}

// MustUInt64 is UInt64 that panics instead of returning an error.
func (v Value) MustUInt64() uint64 {
	u, err := v.UInt64()
	if err != nil {
		panic(err)
	}
	return u
}

// Zero returns the zero satoshi value.
func Zero() Value {
	return Value(0)
}

// From converts any integer into a Value, validating it against the satoshi bounds.
func From[T types.Integer](value T) (Value, error) {
	if err := Validate(value); err != nil {
		return 0, err
	}
	return Value(value), nil
}

// MustFrom is From that panics instead of returning an error.
func MustFrom[T types.Integer](value T) Value {
	v, err := From(value)
	if err != nil {
		panic(err)
	}
	return v
}

// Add returns a+b as a Value, checked against the satoshi bounds.
func Add[A, B types.Integer](a A, b B) (Value, error) {
	satsA, err := From(a)
	if err != nil {
		return 0, err
	}
	satsB, err := From(b)
	if err != nil {
		return 0, err
	}
	c := satsA + satsB
	if err = Validate(c); err != nil {
		return 0, err
	}
	return c, nil
}

// MustAdd is Add that panics instead of returning an error.
func MustAdd[A, B types.Integer](a A, b B) Value {
	added, err := Add(a, b)
	if err != nil {
		panic(err)
	}
	return added
}

// Subtract returns a-b as a Value, checked against the satoshi bounds.
func Subtract[A, B types.Integer](a A, b B) (Value, error) {
	satsA, err := From(a)
	if err != nil {
		return 0, err
	}
	satsB, err := From(b)
	if err != nil {
		return 0, err
	}
	c := satsA - satsB
	if err = validateInt(c); err != nil {
		return 0, err
	}
	return c, nil
}

// MustSubtract is Subtract that panics instead of returning an error.
func MustSubtract[A, B types.Integer](a A, b B) Value {
	subtracted, err := Subtract(a, b)
	if err != nil {
		panic(err)
	}
	return subtracted
}

// Multiply returns a*b as a Value, checked against the satoshi bounds.
func Multiply[A, B types.Integer](a A, b B) (Value, error) {
	satsA, err := From(a)
	if err != nil {
		return 0, err
	}
	satsB, err := From(b)
	if err != nil {
		return 0, err
	}
	c := satsA * satsB
	if err = validateInt(c); err != nil {
		return 0, err
	}
	return c, nil
}

// MustMultiply is Multiply that panics instead of returning an error.
func MustMultiply[A, B types.Integer](a A, b B) Value {
	multiplied, err := Multiply(a, b)
	if err != nil {
		panic(err)
	}
	return multiplied
}

// Sum adds every value in the sequence, checking each running total against the bounds.
func Sum[T types.Integer](values iter.Seq[T]) (Value, error) {
	var err error
	var satsB Value
	val := Zero()

	for it := range values {
		satsB, err = From(it)
		if err != nil {
			return 0, err
		}

		val += satsB
		if err = validateInt(val); err != nil {
			return 0, err
		}
	}
	return val, nil
}

// MustSum is Sum that panics instead of returning an error.
func MustSum[T types.Integer](values iter.Seq[T]) Value {
	sum, err := Sum(values)
	if err != nil {
		panic(err)
	}
	return sum
}

// Equal reports whether a and b are the same satoshi amount.
func Equal[A, B types.Integer](a A, b B) (bool, error) {
	satsA, err := From(a)
	if err != nil {
		return false, err
	}
	satsB, err := From(b)
	if err != nil {
		return false, err
	}
	return satsA == satsB, nil
}

// MustEqual is Equal that panics instead of returning an error.
func MustEqual[A, B types.Integer](a A, b B) bool {
	equal, err := Equal(a, b)
	if err != nil {
		panic(err)
	}
	return equal
}

// Validate reports whether value is within the legal satoshi range.
func Validate[T types.Integer](value T) error {
	switch typed := any(value).(type) {
	case int:
		return validateInt(typed)
	case int64:
		return validateInt(typed)
	case uint:
		return validateUint(typed)
	case uint64:
		return validateUint(typed)
	case Value:
		return validateInt(typed)
	default:
		return validateGeneric(typed)
	}
}

func validateInt[T ~int | ~int64](value T) error {
	if value > primitives.MaxSatoshis {
		return fmt.Errorf("satoshi value %d exceeded max value %d", value, primitives.MaxSatoshis)
	}
	if value < -primitives.MaxSatoshis {
		return fmt.Errorf("satoshi value %d is less than minimum allowed value %d", value, -primitives.MaxSatoshis)
	}
	return nil
}

func validateUint[T ~uint | ~uint64](value T) error {
	if value > primitives.MaxSatoshis {
		return fmt.Errorf("satoshi value %d exceeded max value %d", value, primitives.MaxSatoshis)
	}
	return nil
}

func validateGeneric(value any) error {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return validateInt(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return validateUint(v.Uint())
	case reflect.Invalid, reflect.Bool, reflect.Uintptr, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.Array, reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.String, reflect.Struct, reflect.UnsafePointer:
		return fmt.Errorf("unsupported type in validateGeneric")
	default:
		return fmt.Errorf("unsupported type in validateGeneric")
	}
}
