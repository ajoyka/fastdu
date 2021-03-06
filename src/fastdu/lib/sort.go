package lib

import (
	"sort"
)

// implement sort of map which contains files and sizes

type sortedMap struct {
	m    map[string]int64
	keys []string
}

func (s *sortedMap) Len() int {
	return len(s.m)
}

func (s *sortedMap) Less(i, j int) bool {
	return s.m[s.keys[i]] < s.m[s.keys[j]]
}

func (s *sortedMap) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
}

func SortedKeys(m map[string]int64) []string {
	sm := &sortedMap{}
	sm.m = m

	// collect all keys that will eventually be sorted by value in m
	for key, _ := range m {
		//fmt.Println("key", key)
		sm.keys = append(sm.keys, key)
	}
	//fmt.Println("sorted ", sm.keys)

	sort.Sort(sort.Reverse(sm))
	//sort.Sort(sm)
	return sm.keys
}
