package datatypes

var TwoUid Uid = "two"

type Two struct {
	Base
	BoostId   int  `json:"boost_id"`
	Overpower bool `json:"overpower"`
}

func (d *Two) Clone() DataType {
	return &Two {
		Base {},
		d.BoostId,
		d.Overpower,
	}
}

func (*Two) Sample() DataType {
	return &Two {
		Base {},
		33226,
		true,
	}
}

func (d *Two) Attrs() Attrs {
	return d.Base.Attrs().Merge(Attrs {
		"boost_id":  d.BoostId,
		"overpower": d.Overpower,
	})
}

func (d *Two) SetAttrs(data Attrs) error {
	d.BoostId   = int(data["boost_id"].(float64))
	d.Overpower = data["overpower"].(bool)
	
	return nil
}
