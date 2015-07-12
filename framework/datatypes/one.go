package datatypes

var OneUid Uid = "one"

type One struct {
	Base
	ClientVersion string `json:"client_version"`
	Platform      string `json:"platform"`
	Language      string `json:"language"`
}

func (d *One) Clone() DataType {
	return &One {
		Base {},
		d.ClientVersion,
		d.Platform,
		d.Language,
	}
}

func (*One) Sample() DataType {
	return &One {
		Base {},
		"game-1.0",
		"iphone",
		"danish",
	}
}

func (d *One) Attrs() Attrs {
	return d.Base.Attrs().Merge(Attrs {
		"client_version": d.ClientVersion,
		"platform":       d.Platform,
		"language":       d.Language,
	})
}

func (d *One) SetAttrs(data Attrs) error {
	d.ClientVersion = data["client_version"].(string)
	d.Platform      = data["platform"].(string)
	d.Language      = data["language"].(string)
	
	return nil
}
