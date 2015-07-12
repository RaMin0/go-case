package datatypes

import (
	"time"
)

var Uids = []Uid {
	OneUid,
	TwoUid,
	ThreeUid,
}

var Map = map[Uid]DataType {
	OneUid:   &One{},
	TwoUid:   &Two{},
	ThreeUid: &Three{},
}

type Uid string

type Attrs map[string]interface{}

func (a Attrs) Merge(b Attrs) Attrs {
	for key, value := range b {
		a[key] = value
	}
	
	return a
}

type DataType interface {
	Process()
	
	Clone() DataType
	Sample() DataType
	Attrs() Attrs
	SetAttrs(Attrs) error
}

type Base struct {
	ProcessedAt time.Time `json:"processed_at"`
}

func (d *Base) Process() {
	d.ProcessedAt = time.Now()
}

func (d Base) Attrs() Attrs {
	return Attrs {
		"processed_at": d.ProcessedAt,
	}
}
