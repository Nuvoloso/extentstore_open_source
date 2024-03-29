// Copyright 2019 Tad Lebeck
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stats

import (
	"fmt"
	"sync"
)

// stats is a module that collects numeric stats by name. Values will be
// created on the fly as they are manipulated. You must call the Init
// routine before atempting to set any values.  Users should be careful
// to also use pointers to the stat struct or the mutex here will not work.
//
// type MyState struct {
//    mystats *stats.Stats
// }
//
// s := MyState{mystats: &stats.Stats{}
// s.mystats.Inc("differences")
// s.mystats.Set("threads", 5)
// s.mystats.Report()

// Stats is a collector of statistics
type Stats struct {
	mux     sync.Mutex
	statMap map[string]int
}

// Init must be called on a Stats struct before using it
func (sm *Stats) Init() {
	sm.mux.Lock()
	sm.statMap = make(map[string]int)
	sm.mux.Unlock()
}

// Inc increments a stat of the given name
func (sm *Stats) Inc(name string) {
	sm.mux.Lock()
	sm.statMap[name]++
	sm.mux.Unlock()
}

// Dec decrements a stat of the given name
func (sm *Stats) Dec(name string) {
	sm.mux.Lock()
	sm.statMap[name]--
	sm.mux.Unlock()
}

// Add adds the given value to the statistic of the given name
func (sm *Stats) Add(name string, val int) {
	sm.mux.Lock()
	sm.statMap[name] += val
	sm.mux.Unlock()
}

// Sub subracts the given value from the statistic of the given name
func (sm *Stats) Sub(name string, val int) {
	sm.mux.Lock()
	sm.statMap[name] -= val
	sm.mux.Unlock()
}

// Set sets the given value to the statistic of the given name
func (sm *Stats) Set(name string, val int) {
	sm.mux.Lock()
	sm.statMap[name] = val
	sm.mux.Unlock()
}

// Get gets the statistic value for the given name
func (sm *Stats) Get(name string) int {
	sm.mux.Lock()
	defer sm.mux.Unlock()
	return sm.statMap[name]
}

// Report displays all of the statistics and their value
func (sm *Stats) Report(header string) {
	sm.mux.Lock()
	fmt.Println(header)
	for a, b := range sm.statMap {
		fmt.Printf("    %s %d\n", a, b)
	}
	sm.mux.Unlock()
}
