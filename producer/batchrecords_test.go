package ministreamproducer

import (
	"reflect"
	"testing"
)

func TestNewBatchRecords(t *testing.T) {
	type args struct {
		maxCapacity int
	}
	tests := []struct {
		name string
		args args
		want *BatchRecords
	}{
		{
			name: "TestNewBatchRecords",
			args: args{
				maxCapacity: 10,
			},
			want: &BatchRecords{
				id:      0,
				records: make([]interface{}, 0, 10),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBatchRecords(tt.args.maxCapacity); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBatchRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBatchRecords(t *testing.T) {
	maxCapacity := 10
	batchRecords := NewBatchRecords(maxCapacity)
	if batchRecords.IsEmpty() != true {
		t.Errorf("IsEmpty() = %v, want %v", batchRecords.IsEmpty(), true)
	}
	if batchRecords.IsFull() != false {
		t.Errorf("IsFull() = %v, want %v", batchRecords.IsFull(), false)
	}
	if batchRecords.Size() != 0 {
		t.Errorf("Size() = %v, want %v", batchRecords.Size(), 0)
	}
	if len(batchRecords.GetRecords()) != 0 {
		t.Errorf("GetRecords() = %v, want %v", len(batchRecords.GetRecords()), 0)
	}

	for i := 0; i < maxCapacity-1; i++ {
		record := i
		batchRecords.Append(record)
		if batchRecords.IsEmpty() != false {
			t.Errorf("IsEmpty() = %v, want %v", batchRecords.IsEmpty(), false)
		}
		if batchRecords.IsFull() != false {
			t.Errorf("IsFull() = %v, want %v", batchRecords.IsFull(), false)
		}
		if batchRecords.Size() != i+1 {
			t.Errorf("Size() = %v, want %v", batchRecords.Size(), i+1)
		}
		if len(batchRecords.GetRecords()) != i+1 {
			t.Errorf("GetRecords() = %v, want %v", len(batchRecords.GetRecords()), i+1)
		}
	}

	// Append one more record
	record := maxCapacity - 1
	batchRecords.Append(record)

	if batchRecords.IsEmpty() != false {
		t.Errorf("IsEmpty() = %v, want %v", batchRecords.IsEmpty(), false)
	}
	if batchRecords.IsFull() != true {
		t.Errorf("IsFull() = %v, want %v", batchRecords.IsFull(), true)
	}
	if batchRecords.Size() != maxCapacity {
		t.Errorf("Size() = %v, want %v", batchRecords.Size(), maxCapacity)
	}
	records := batchRecords.GetRecords()
	if len(records) != maxCapacity {
		t.Errorf("GetRecords() = %v, want %v", len(batchRecords.GetRecords()), maxCapacity)
	}
	for i := 0; i < maxCapacity; i++ {
		if records[i] != i {
			t.Errorf("GetRecords() = %v, want %v", records[i], i)
		}
	}

	// Clear the records
	batchRecords.Clear()
	if batchRecords.IsEmpty() != true {
		t.Errorf("IsEmpty() = %v, want %v", batchRecords.IsEmpty(), true)
	}
	if batchRecords.IsFull() != false {
		t.Errorf("IsFull() = %v, want %v", batchRecords.IsFull(), false)
	}
	if batchRecords.Size() != 0 {
		t.Errorf("Size() = %v, want %v", batchRecords.Size(), 0)
	}
	if len(batchRecords.GetRecords()) != 0 {
		t.Errorf("GetRecords() = %v, want %v", len(batchRecords.GetRecords()), 0)
	}
}
