package framework

import (
	"log"
)

var (
	DataSource Source = NewDefaultDataSource()
	ResultSink Sink   = NewDefaultResultSink()
)

type NewWorkerFunc func(<-chan *InData, chan *OutData) Worker

type Worker interface {
	Run() error
}

type Framework struct {
	newWorkerFunc NewWorkerFunc
}

func New(newWorkerFunc NewWorkerFunc) *Framework {
	return &Framework{
		newWorkerFunc: newWorkerFunc,
	}
}

func (f *Framework) Run() {
	log.Printf("[FRAMEWORK] Starting...")

	for {
		// create input, output and termination channels
		inDataChan := make(chan *InData)
		outDataChan := make(chan *OutData)
		dataSourceTermChan := make(chan struct{}, 1)
		resultSinkTermChan := make(chan struct{}, 1)

		// create worker
		w := f.newWorkerFunc(inDataChan, outDataChan)

		// initiate data source and result sink
		DataSource.Fill(inDataChan, dataSourceTermChan)
		ResultSink.Drain(outDataChan, resultSinkTermChan)

		// start execution
		err := w.Run()

		// execution ended, shutdown source and sink
		dataSourceTermChan <- struct{}{}
		close(dataSourceTermChan)
		resultSinkTermChan <- struct{}{}
		close(resultSinkTermChan)

		if err == nil {
			// no error, stop execution
			break
		}
		log.Printf("[FRAMEWORK] Worker stopped, restarting it: %v", err)
	}
}
