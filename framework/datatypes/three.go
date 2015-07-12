package datatypes

import (
	"time"
)

var ThreeUid Uid = "three"

type Three struct {
	Base
	Timestamp time.Time `json:"timestamp"`
}

func (d *Three) Clone() DataType {
	return &Three {
		Base {},
		d.Timestamp,
	}
}

func (*Three) Sample() DataType {
	timestamp, _ := time.Parse(time.RFC3339Nano, "2006-01-02T15:04:05.999Z")
	
	return &Three {
		Base {},
		timestamp,
	}
}

func (d *Three) Attrs() Attrs {
	return d.Base.Attrs().Merge(Attrs {
		"timestamp": d.Timestamp,
	})
}

func (d *Three) SetAttrs(data Attrs) error {
	d.Timestamp, _ = time.Parse(time.RFC3339Nano, data["timestamp"].(string))
	
	return nil
}
