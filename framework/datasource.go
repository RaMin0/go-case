package framework

import (
	"math/rand"
	"sync"
)

const (
	InDataCount       = 100
	RngSeed     int64 = 1234
)

var (
	DataTypeOne   = []byte(`{"type":"one","data":{"client_version":"game-1.0","platform":"iphone","language":"danish"}}`)
	DataTypeTwo   = []byte(`{"type":"two","data":{"boost_id":33226,"overpower":true}}`)
	DataTypeThree = []byte(`{"type":"three","data":{"timestamp":"2006-01-02T15:04:05.999Z"}}`)
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
		running:                false,
		lock:                   &sync.Mutex{},
	}
}

type DefaultDataSource struct {
	running                bool
	startingSequenceNumber int
	lock                   *sync.Mutex
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
		rng := rand.New(rand.NewSource(RngSeed))

		// advance rng to starting sequence number
		for i := -1; i < s.startingSequenceNumber; i++ {
			_ = rng.Int()
		}
		currentSequenceNumber := s.startingSequenceNumber + 1

		// generate data
		for currentSequenceNumber < InDataCount {
			data := &InData{
				SequenceNumber: currentSequenceNumber,
			}

			// get random number between 0 and 99
			i := rng.Intn(100)
			switch {
			case i < 20:
				data.Data = DataTypeOne
			case i < 75:
				data.Data = DataTypeTwo
			case i < 90:
				data.Data = DataTypeThree
			default:
				data = nil
			}

			select {
			case <-term:
				// terminate
				return
			case in <- data:
				// do nothing
			}

			currentSequenceNumber++
		}

		// shutdown
		close(in)
	}()
}
