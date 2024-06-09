package ministreamproducer

import "testing"

func TestBuildCircularBuffer(t *testing.T) {
	buf := BuildCircularBuffer(10)
	if buf == nil {
		t.Fatalf("creation failed")
	}
}

func TestCapacity(t *testing.T) {
	buf := BuildCircularBuffer(10)
	if buf.capacity != 10 {
		t.Fatalf("wrong capacity value")
	}
}

func TestPush(t *testing.T) {
	buf := BuildCircularBuffer(3)
	if !buf.Push(1) {
		t.Fatalf("push failed 1")
	}
	if !buf.Push(2) {
		t.Fatalf("push failed 2")
	}
	if buf.Push(3) {
		t.Fatalf("must not be able to push")
	}
}

func TestPop(t *testing.T) {
	buf := BuildCircularBuffer(3)
	if _, ok := buf.Pop(); ok {
		t.Fatalf("must not be able to pop")
	}
	if !buf.Push(1) {
		t.Fatalf("push failed 1")
	}
	if !buf.Push(2) {
		t.Fatalf("push failed 2")
	}
	if _, ok := buf.Pop(); !ok {
		t.Fatalf("must be able to pop")
	}
	if _, ok := buf.Pop(); !ok {
		t.Fatalf("must be able to pop")
	}
	if _, ok := buf.Pop(); ok {
		t.Fatalf("must not be able to pop")
	}
	if !buf.Push(3) {
		t.Fatalf("must be able to push")
	}
	if _, ok := buf.Pop(); !ok {
		t.Fatalf("must be able to pop")
	}
	if _, ok := buf.Pop(); ok {
		t.Fatalf("must not be able to pop")
	}
}

func TestIsEmpty(t *testing.T) {
	buf := BuildCircularBuffer(3)
	if !buf.IsEmpty() {
		t.Fatalf("must be empty")
	}
	if !buf.Push(1) {
		t.Fatalf("push failed 1")
	}
	if buf.IsEmpty() {
		t.Fatalf("must not be empty")
	}
	if _, ok := buf.Pop(); !ok {
		t.Fatalf("must be able to pop")
	}
	if !buf.IsEmpty() {
		t.Fatalf("must be empty")
	}
}

func TestIsFull(t *testing.T) {
	buf := BuildCircularBuffer(3)
	if buf.IsFull() {
		t.Fatalf("must not be full")
	}
	if !buf.Push(1) {
		t.Fatalf("push failed 1")
	}
	if buf.IsFull() {
		t.Fatalf("must not be full")
	}
	if !buf.Push(2) {
		t.Fatalf("push failed 1")
	}
	if !buf.IsFull() {
		t.Fatalf("must be full")
	}
	if _, ok := buf.Pop(); !ok {
		t.Fatalf("must be able to pop")
	}
	if buf.IsFull() {
		t.Fatalf("must not be full")
	}
}

func TestSize(t *testing.T) {
	buf := BuildCircularBuffer(3)
	if buf.Size() != 0 {
		t.Fatalf("invalid size value")
	}
	if !buf.Push(1) {
		t.Fatalf("push failed 1")
	}
	if buf.Size() != 1 {
		t.Fatalf("invalid size value")
	}
	if !buf.Push(2) {
		t.Fatalf("push failed 1")
	}
	if buf.Size() != 2 {
		t.Fatalf("invalid size value")
	}
	if _, ok := buf.Pop(); !ok {
		t.Fatalf("must be able to pop")
	}
	if buf.Size() != 1 {
		t.Fatalf("invalid size value")
	}
	if _, ok := buf.Pop(); !ok {
		t.Fatalf("must be able to pop")
	}
	if buf.Size() != 0 {
		t.Fatalf("invalid size value")
	}
}

func TestClear(t *testing.T) {
	buf := BuildCircularBuffer(3)
	buf.Push(1)
	buf.Push(2)
	if buf.IsEmpty() {
		t.Fatalf("must not be empty")
	}
	buf.Clear()
	if !buf.IsEmpty() {
		t.Fatalf("must be empty")
	}
}

func TestFullCapacity(t *testing.T) {
	var ok bool
	var value interface{}
	batchSize := 10000
	capacity := batchSize + 1

	// create a circular buffer
	buf := BuildCircularBuffer(capacity)
	if buf.Capacity() != capacity {
		t.Fatalf("invalid capacity value")
	}
	if buf.AvailableCapacity() != capacity-1 {
		t.Fatalf("invalid available capacity value")
	}
	if buf.IsFull() {
		t.Fatalf("must not be full")
	}
	if !buf.IsEmpty() {
		t.Fatalf("must be empty")
	}
	if buf.Size() != 0 {
		t.Fatalf("invalid size value")
	}

	// fill the buffer with items
	for i := 0; i < batchSize; i++ {
		if !buf.Push(i) {
			t.Fatalf("push failed")
		}
	}

	// check the buffer state
	if buf.Capacity() != capacity {
		t.Fatalf("invalid capacity value")
	}
	if buf.IsEmpty() {
		t.Fatalf("must not be empty")
	}
	if buf.Size() != batchSize {
		t.Fatalf("invalid size value")
	}
	if buf.IsFull() {
		if capacity > batchSize+1 {
			t.Fatalf("must not be full")
		}
	} else {
		if capacity == batchSize+1 {
			t.Fatalf("must be full")
		}
	}
	if buf.AvailableCapacity() != capacity-batchSize-1 {
		t.Fatalf("invalid available capacity value")
	}

	// pop all the items from the buffer
	for i := 0; i < batchSize; i++ {
		if value, ok = buf.Pop(); !ok {
			t.Fatalf("pop failed")
		}
		if value != i {
			t.Fatalf("invalid value")
		}
	}

	// check the buffer state
	if buf.Capacity() != capacity {
		t.Fatalf("invalid capacity value")
	}
	if buf.IsFull() {
		t.Fatalf("must not be full")
	}
	if !buf.IsEmpty() {
		t.Fatalf("must be empty")
	}
	if buf.Size() != 0 {
		t.Fatalf("invalid size value")
	}
	if buf.AvailableCapacity() != capacity-1 {
		t.Fatalf("invalid available capacity value")
	}

	// fill the buffer with items
	for i := 0; i < batchSize; i++ {
		if !buf.Push(i + batchSize) {
			t.Fatalf("push failed")
		}
	}

	// check the buffer state
	if buf.Capacity() != capacity {
		t.Fatalf("invalid capacity value")
	}
	if buf.IsFull() {
		if capacity > batchSize+1 {
			t.Fatalf("must not be full")
		}
	} else {
		if capacity == batchSize+1 {
			t.Fatalf("must be full")
		}
	}
	if buf.IsEmpty() {
		t.Fatalf("must not be empty")
	}
	if buf.Size() != batchSize {
		t.Fatalf("invalid size value")
	}
	if buf.AvailableCapacity() != capacity-batchSize-1 {
		t.Fatalf("invalid available capacity value")
	}

	// pop all the items from the buffer
	for i := 0; i < batchSize; i++ {
		if value, ok = buf.Pop(); !ok {
			t.Fatalf("pop failed")
		}
		if value != i+batchSize {
			t.Fatalf("invalid value")
		}
	}
}
