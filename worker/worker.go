package worker

import (
	"github.com/fantyz/go-case/framework"
	"log"
)

type Worker struct {
	inChan  <-chan *framework.InData
	outChan chan *framework.OutData
}

func New(in <-chan *framework.InData, out chan *framework.OutData) framework.Worker {
	return &Worker{
		inChan:  in,
		outChan: out,
	}
}

func (w *Worker) Run() error {
	log.Println("Starting worker")

	// read from in channel
	_ = <-w.inChan

	// write results to result channel
	w.outChan <- &framework.OutData{}

	log.Println("Worker finished")
	return nil
}
