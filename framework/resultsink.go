package framework

type OutData struct{}

type Sink interface {
	Drain(<-chan *OutData, <-chan struct{})
}

func NewDefaultResultSink() Sink {
	return &DefaultResultSink{}
}

type DefaultResultSink struct{}

func (s *DefaultResultSink) Drain(out <-chan *OutData, term <-chan struct{}) {
	go func() {
		for {
			select {
			case _, ok := <-out:
				if !ok {
					// channel closed, all done
					return
				}
			case <-term:
				// terminate
				return
			}
		}
	}()

}
