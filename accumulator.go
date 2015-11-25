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