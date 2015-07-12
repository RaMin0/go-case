package worker

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"github.com/fantyz/go-case/framework"
	"github.com/fantyz/go-case/framework/datatypes"
	"log"
	"time"
)

const (
	WorkerBufferMaxSize = 1000
	WorkerBufferMaxTime = 5 * time.Second
)

type Worker struct {
	inChan  <-chan *framework.InData
	outChan chan *framework.OutData
}

func New(in <-chan *framework.InData, out chan *framework.OutData) framework.Worker {
	// Example: Enable the data source to send nil records
	// framework.DataSource.EnableNilRecords(0.0001)

	return &Worker{
		inChan:  in,
		outChan: out,
	}
}

var (
	buffer *BufferedOutData = NewBuffer()
)

type BufferedOutData struct {
	createdAt          time.Time
	size               int
	lastSequenceNumber int
	outData            map[datatypes.Uid][]datatypes.Attrs
}

func NewBuffer() *BufferedOutData {
	newBuffer := BufferedOutData {}
	newBuffer.Reset()
	return &newBuffer
}

func (b *BufferedOutData) Push(dataTypeUid datatypes.Uid, sequenceNumber int, dataType datatypes.DataType, dataTypeAttrs datatypes.Attrs) {
	b.size++
	b.lastSequenceNumber = sequenceNumber
	
	if err := dataType.SetAttrs(dataTypeAttrs); err != nil {
		log.Println("[Worker] %v, while parsing: %v\n", err, dataTypeUid)
		return
	}
	
	dataType.Process()
	b.outData[dataTypeUid] = append(b.outData[dataTypeUid], dataType.Attrs())
}

func (b *BufferedOutData) Flush() *framework.OutData {
	outData := framework.OutData {
		SequenceNumber: b.lastSequenceNumber,
		Data:           make(map[datatypes.Uid][]byte, 0),
	}
	
	for dataType, inData := range b.outData {
		json, _ := json.Marshal(inData)
		
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		w.Write(json)
		w.Close()
		outData.Data[dataType] = b.Bytes()
	}
	
	b.Reset()
	
	return &outData
}

func (b *BufferedOutData) Reset() {
	b.createdAt = time.Now()
	b.size = 0
	b.lastSequenceNumber = 0
	b.outData = make(map[datatypes.Uid][]datatypes.Attrs)
}

func (w *Worker) Run() error {
	log.Println("[Worker] Starting...")

	// read from in channel
	for in := range w.inChan {
		if in == nil {
			// nil indicating worker should simulate a crash by returning an error
			return errors.New("nil data received, terminating")
		}
		
		var inJson map[string]interface{}
		if err := json.Unmarshal(in.Data, &inJson); err != nil {
			log.Printf("[Worker] %v\n", err)
			continue
		}
		
		dataTypeUid := datatypes.Uid(inJson["type"].(string))
		dataTypeAttrs := datatypes.Attrs(inJson["data"].(map[string]interface{}))
		buffer.Push(dataTypeUid, in.SequenceNumber, datatypes.Map[dataTypeUid].Clone(), dataTypeAttrs)
		
		// write results to result channel
		if time.Since(buffer.createdAt) >= WorkerBufferMaxTime || buffer.size >= WorkerBufferMaxSize {
			w.outChan <- buffer.Flush()
		}
		
//		time.Sleep(500 * time.Millisecond)
	}

	log.Println("[Worker] finished")
	return nil
}
