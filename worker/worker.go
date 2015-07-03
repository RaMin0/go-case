package worker

import (
	"errors"
	"github.com/fantyz/go-case/framework"
	"log"
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

func (w *Worker) Run() error {
	log.Println("Starting worker")

	// read from in channel
	for in := range w.inChan {
		if in == nil {
			// nil indicating worker should simulate a crash by returning an error
			return errors.New("nil data received, terminating")
		}

		// write results to result channel
		w.outChan <- &framework.OutData{}
	}

	log.Println("Worker finished")
	return nil
}
