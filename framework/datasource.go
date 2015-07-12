package framework

import (
	"encoding/json"
	"github.com/fantyz/go-case/framework/datatypes"
	"log"
	"math/rand"
	"sync"
)

const (
	InDataCount = 10000000

	DataGenerationSeed    int64 = 1234
	NilDataGenerationSeed int64 = 5678
)

type InData struct {
	SequenceNumber int
	Data           []byte
}

type Source interface {
	Fill(chan *InData, <-chan struct{})
	EnableNilRecords(float32)
	SetStartingSequenceNumber(int)
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
		log.Printf("[DataSource] Unable to set starting sequence number when running (cannot be called while running a worker)")
		return
	}
	s.startingSequenceNumber = sequenceNumber
}

func (s *DefaultDataSource) Fill(in chan *InData, term <-chan struct{}) {
	s.lock.Lock()
	if s.running {
		log.Printf("[DataSource] Already running!")
		return
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
				i := dataGenerationRng.Intn(len(datatypes.Map))
				randomUid := datatypes.Uids[i]
				
				data.Data, _ = json.Marshal(map[string]interface{} {
					"type": randomUid,
					"data": datatypes.Map[randomUid].Sample().Attrs(),  
				})

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
