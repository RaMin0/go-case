package external

import (
	"sync"
)

var (
	lock   sync.Mutex
	values = map[string]int{}
)

func SetValue(key string, val int) {
	lock.Lock()
	values[key] = val
	lock.Unlock()
}

func GetValue(key string) (val int, found bool) {
	lock.Lock()
	val, found = values[key]
	lock.Unlock()
	return
}

func UpdateValue(key string, old, new int) (success bool) {
	lock.Lock()
	defer lock.Unlock()
	val, found := values[key]
	if !found || val != old {
		return false
	}
	values[key] = new
	return true
}
