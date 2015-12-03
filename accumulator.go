package main

import (
	"encoding/binary"
	"bytes"
	"math"
	"fmt"
)

type Accumulator interface{
	Column() string
	Add(data []byte) error
	CanAccumulateType(fieldType string) bool
}

type AverageAccumulator struct{
	Col string

	sum float64
	ct int64
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

type CountAccumulator struct{
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

type MinAccumulator struct{
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

type MaxAccumulator struct{
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

type DebugAccumulator struct{
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

func (da *DebugAccumulator) CanAccumulateType(fieldType string) bool {
	return true
}