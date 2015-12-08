package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

type Accumulator interface {
	Column() string
	Add(data []byte) error
	CanAccumulateType(fieldType string) bool
	Reduce(acc Accumulator) error
}

type AverageAccumulator struct {
	Col string

	sum float64
	ct  int64
}

func (aa *AverageAccumulator) Column() string {
	return aa.Col
}

func (aa *AverageAccumulator) Add(data []byte) error {

	var number float64
	binary.Read(bytes.NewReader(data), binary.BigEndian, &number)
	if !math.IsNaN(number) {
		aa.sum += number
		aa.ct += 1
	} else {
		// TODO: we should not save NaN values to the db
		// return fmt.Errorf("field not a number: %v", data)
	}

	return nil
}

func (a *AverageAccumulator) Reduce(acc Accumulator) error {

	if b, ok := acc.(*AverageAccumulator); ok {
		a.sum += b.sum
		a.ct += b.ct
	} else {
		return fmt.Errorf("ERROR: Got the wrong type in reduce function")
	}

	return nil

}

func (aa *AverageAccumulator) CanAccumulateType(fieldType string) bool {
	switch fieldType {
	case "int":
		fallthrough
	case "float":
		return true
	case "string":
		fallthrough
	default:
		return false
	}
}

type CountAccumulator struct {
	Col string

	ct int64
}

func (aa *CountAccumulator) Column() string {
	return aa.Col
}

func (aa *CountAccumulator) Add(data []byte) error {
	aa.ct += 1
	return nil
}

func (ca *CountAccumulator) CanAccumulateType(fieldType string) bool {
	return true
}

func (ca *CountAccumulator) Reduce(acc Accumulator) error {
	if b, ok := acc.(*CountAccumulator); ok {
		ca.ct += b.ct
	} else {
		return fmt.Errorf("ERROR: Got the wrong type in reduce function")
	}
	return nil
}

type MinAccumulator struct {
	Col string

	min float64
}

func (aa *MinAccumulator) Column() string {
	return aa.Col
}

func (aa *MinAccumulator) Add(data []byte) error {
	var number float64
	binary.Read(bytes.NewReader(data), binary.BigEndian, &number)
	if !math.IsNaN(number) && number < aa.min {
		aa.min = number
	} else {
		// TODO: we should not save NaN values to the db
		// return fmt.Errorf("field not a number: %v", data)
	}

	return nil
}

func (ma *MinAccumulator) Reduce(acc Accumulator) error {
	if b, ok := acc.(*MinAccumulator); ok {
		if ma.min > b.min {
			ma.min = b.min
		}
	} else {
		return fmt.Errorf("ERROR: Got the wrong type in reduce function")
	}
	return nil
}

func (ma *MinAccumulator) CanAccumulateType(fieldType string) bool {
	switch fieldType {
	case "int":
		fallthrough
	case "float":
		return true
	case "string":
		fallthrough
	default:
		return false
	}
}

type MaxAccumulator struct {
	Col string

	max float64
}

func (aa *MaxAccumulator) Column() string {
	return aa.Col
}

func (aa *MaxAccumulator) Add(data []byte) error {
	var number float64
	binary.Read(bytes.NewReader(data), binary.BigEndian, &number)
	if !math.IsNaN(number) && number > aa.max {
		aa.max = number
	} else {
		// TODO: we should not save NaN values to the db
		// return fmt.Errorf("field not a number: %v", data)
	}

	return nil
}

func (ma *MaxAccumulator) Reduce(acc Accumulator) error {
	if b, ok := acc.(*MaxAccumulator); ok {
		if ma.max < b.max {
			ma.max = b.max
		}
	} else {
		return fmt.Errorf("ERROR: Got the wrong type in reduce function")
	}
	return nil
}

func (ma *MaxAccumulator) CanAccumulateType(fieldType string) bool {
	switch fieldType {
	case "int":
		fallthrough
	case "float":
		return true
	case "string":
		fallthrough
	default:
		return false
	}
}

type DebugAccumulator struct {
	Col string

	ct int64
}

func (aa *DebugAccumulator) Column() string {
	return aa.Col
}

func (aa *DebugAccumulator) Add(data []byte) error {
	fmt.Printf("% 3d  %s\n", aa.ct, data)
	aa.ct += 1
	return nil
}

func (da *DebugAccumulator) Reduce(acc Accumulator) error {
	fmt.Println("Reducing!!!")
	return nil
}

func (da *DebugAccumulator) CanAccumulateType(fieldType string) bool {
	return true
}
