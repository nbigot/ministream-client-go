package ministreamproducer

import (
	"sync"
)

type CircularBuffer struct {
	items           []interface{}
	capacity        int
	nextWriteCursor int
	nextReadCursor  int
	mu              sync.Mutex
}

func BuildCircularBuffer(capacity int) *CircularBuffer {
	// note: the maximum number of items in the buffer is equal to capacity - 1
	if capacity < 2 {
		panic("capacity must be > 1")
	}
	return &CircularBuffer{items: make([]interface{}, capacity), capacity: capacity, nextWriteCursor: 0, nextReadCursor: 0}
}

func (c *CircularBuffer) Clear() {
	c.nextWriteCursor = 0
	c.nextReadCursor = 0
}

func (c *CircularBuffer) Capacity() int {
	return c.capacity
}

func (c *CircularBuffer) IsEmpty() bool {
	return c.nextReadCursor == c.nextWriteCursor
}

func (c *CircularBuffer) IsFull() bool {
	return (c.nextWriteCursor+1)%c.capacity == c.nextReadCursor
}

func (c *CircularBuffer) Size() int {
	if c.nextWriteCursor >= c.nextReadCursor {
		return c.nextWriteCursor - c.nextReadCursor
	} else {
		return c.capacity - c.nextReadCursor + c.nextWriteCursor
	}
}

func (c *CircularBuffer) AvailableCapacity() int {
	return c.Capacity() - c.Size() - 1
}

func (c *CircularBuffer) Push(item interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.IsFull() {
		return false
	}

	c.items[c.nextWriteCursor] = item
	c.nextWriteCursor = (c.nextWriteCursor + 1) % c.capacity
	return true
}

func (c *CircularBuffer) PushItems(items []interface{}, indexBegin int, indexEnd int) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.IsFull() {
		return 0
	}

	cptItemsToPush := indexEnd - indexBegin + 1
	availableCapacity := c.AvailableCapacity()
	if availableCapacity < cptItemsToPush {
		cptItemsToPush = availableCapacity
	}

	for i := 0; i < cptItemsToPush; i++ {
		c.items[c.nextWriteCursor] = items[indexBegin+i]
		c.nextWriteCursor = (c.nextWriteCursor + 1) % c.capacity
	}

	return cptItemsToPush
}

func (c *CircularBuffer) Pop() (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.IsEmpty() {
		return nil, false
	}

	idx := c.nextReadCursor
	c.nextReadCursor = (c.nextReadCursor + 1) % c.capacity
	return c.items[idx], true
}
