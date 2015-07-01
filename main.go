package main

import (
	"github.com/fantyz/go-case/framework"
	"github.com/fantyz/go-case/worker"
)

func main() {
	f := framework.New(worker.New)
	f.Run()
}
