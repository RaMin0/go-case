package framework

import (
	"math/rand"
	"sync"
)

const (
	InDataCount = 10000000

	DataGenerationSeed    int64 = 1234
	NilDataGenerationSeed int64 = 5678
)

type DataType string

var (
	DataTypeOne   DataType = "one"
	DataTypeTwo   DataType = "two"
	DataTypeThree DataType = "three"
)

var (
	SampleDataTypeOne   = []byte(`{"type":"` + DataTypeOne + `","data":{"client_version":"game-1.0","platform":"iphone","language":"danish"}}`)
	SampleDataTypeTwo   = []byte(`{"type":"` + DataTypeTwo + `","data":{"boost_id":33226,"overpower":true}}`)
	SampleDataTypeThree = []byte(`{"type":"` + DataTypeThree + `","data":{"timestamp":"2006-01-02T15:04:05.999Z"}}`)
)

type InData struct {
	SequenceNumber int
	Data           []byte
}

type Source interface {
	Fill(chan *InData, <-chan struct{})
}

func NewDefaultDataSource() Source {
	return &DefaultDataSource{
		startingSequenceNumber: -1,
		nilRecords:             0,
		running:                false,
		lock:                   &sync.Mutex{},
	}
}

type DefaultDataSource struct {
	nilRecords             float32
	running                bool
	startingSequenceNumber int
	lock                   *sync.Mutex
}

func (s *DefaultDataSource) EnableNilRecords(frequency float32) {
	s.nilRecords = frequency
}

func (s *DefaultDataSource) SetStartingSequenceNumber(sequenceNumber int) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.running {
		panic("unable to set starting sequence number when running (cannot be called while running a worker)")
	}
	s.startingSequenceNumber = sequenceNumber
}

func (s *DefaultDataSource) Fill(in chan *InData, term <-chan struct{}) {
	s.lock.Lock()
	if s.running {
		panic("data source is already running")
	}
	s.running = true
	s.lock.Unlock()

	go func() {
		defer func() {
			s.lock.Lock()
			s.running = false
			s.lock.Unlock()
		}()

		// initialize rng using a specific seed to create a deterministic sequence
		dataGenerationRng := rand.New(rand.NewSource(DataGenerationSeed))
		nilGenerationRng := rand.New(rand.NewSource(NilDataGenerationSeed))

		// advance rngs to starting sequence number
		i := -1
		for i < s.startingSequenceNumber {
			if s.nilRecords <= nilGenerationRng.Float32() {
				i++
				_ = dataGenerationRng.Intn(100)
			}
		}
		currentSequenceNumber := s.startingSequenceNumber + 1

		// generate data
		for currentSequenceNumber < InDataCount {
			data := &InData{
				SequenceNumber: currentSequenceNumber,
			}

			if s.nilRecords > nilGenerationRng.Float32() {
				// insert nil data
				data = nil
			} else {
				// determine which data should be used
				i := dataGenerationRng.Intn(100)
				switch {
				case i < 20:
					data.Data = SampleDataTypeOne
				case i < 75:
					data.Data = SampleDataTypeTwo
				default:
					data.Data = SampleDataTypeThree
				}

				// increment currentSequenceNumber
				currentSequenceNumber++
			}

			select {
			case <-term:
				// terminate
				return
			case in <- data:
				// do nothing
			}
		}

		// shutdown
		close(in)
	}()
}
