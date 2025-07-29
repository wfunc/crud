package sqlx

import (
	"fmt"
	"reflect"
	"regexp"
	"sync"
	"testing"

	"github.com/wfunc/util/xmap"
)

// ErrMock is the error returned when a mock is triggered.
var ErrMock = fmt.Errorf("mock error")

// Verbose indicates whether to print verbose output during mocking.
var Verbose = false

var mocking = false
var mockPanic = false
var mockTrigger = map[string][]int{}
var mockMatch = map[string]*regexp.Regexp{}
var mockRunned = map[string]int{}
var mockRunnedLck = sync.RWMutex{}

func mockerCheck(key, sql string) (err error) {
	if mocking {
		mockRunnedLck.Lock()
		mockRunned[key]++
		trigger := mockTrigger[key]
		runned := mockRunned[key]
		if trigger != nil && (trigger[0] < 0 || (trigger[0] <= runned && runned <= trigger[1])) {
			err = ErrMock
		}
		match := mockMatch[key]
		if match != nil && match.MatchString(sql) {
			err = ErrMock
		}
		if Verbose {
			fmt.Printf("Mocking %v trigger:%v,runned:%v,err:%v,sql:\n%v\n", key, mockTrigger[key], mockRunned[key], err, sql)
		}
		mockRunnedLck.Unlock()
		if mockPanic && err != nil {
			panic(err)
		}
	}
	return
}

// MockerStart starts the mocking process.
func MockerStart() {
	mocking = true
}

// MockerStop stops the mocking process and clears the mock state.
func MockerStop() {
	MockerClear()
	mocking = false
}

// MockerClear clears the mock state, resetting all triggers and matches.
func MockerClear() {
	mockRunnedLck.Lock()
	mockTrigger = map[string][]int{}
	mockMatch = map[string]*regexp.Regexp{}
	mockRunned = map[string]int{}
	mockPanic = false
	mockRunnedLck.Unlock()
}

func mockerSet(key, match string, isPanice bool, triggers ...int) {
	mockRunnedLck.Lock()
	defer mockRunnedLck.Unlock()
	if len(match) > 0 {
		mockMatch[key] = regexp.MustCompile(match)
	} else {
		if len(triggers) == 1 {
			mockTrigger[key] = []int{triggers[0], triggers[0]}
		} else if len(triggers) > 1 {
			mockTrigger[key] = triggers
		} else {
			panic("trigger is required")
		}
	}
	mockPanic = isPanice
}

// MockerCaller is a struct that holds the call function and shoulder for mocking.
type MockerCaller struct {
	Call     func(func(trigger int) (res xmap.M, err error)) xmap.M
	Shoulder xmap.Shoulder
}

// Should is a method to set expectations for the mock.
func (m *MockerCaller) Should(t *testing.T, args ...any) *MockerCaller {
	m.Shoulder.Should(t, args...)
	return m
}

// ShouldError is a method to set expectations for the mock to return an error.
func (m *MockerCaller) ShouldError(t *testing.T) *MockerCaller {
	m.Shoulder.ShouldError(t)
	return m
}

// OnlyLog is a method to set whether the mock should only log the call without executing it.
func (m *MockerCaller) OnlyLog(only bool) *MockerCaller {
	m.Shoulder.OnlyLog(only)
	return m
}

// Should is a method to set expectations for the mock.
func Should(t *testing.T, key string, v any) (caller *MockerCaller) {
	caller = &MockerCaller{}
	caller.Should(t, key, v).Call = func(call func(trigger int) (res xmap.M, err error)) xmap.M {
		res, err := call(0)
		caller.Shoulder.Valid(4, res, err)
		return res
	}
	return
}

func rangeArgs(args []any, call func(key string, trigger int)) {
	triggerAll := map[string][]int{}
	triggerKeys := []string{}
	triggerAdd := false
	for i, arg := range args {
		switch arg := arg.(type) {
		case string:
			if triggerAdd {
				triggerKeys = []string{}
			}
			triggerAdd = false
			triggerKeys = append(triggerKeys, arg)
		case int:
			triggerAdd = true
			for _, key := range triggerKeys {
				triggerAll[key] = append(triggerAll[key], arg)
			}
		default:
			panic(fmt.Sprintf("args[%v] is %v and not supported", i, reflect.TypeOf(arg)))
		}
	}
	for key, triggers := range triggerAll {
		for _, trigger := range triggers {
			call(key, trigger)
		}
	}
}

// MockerSet sets a mock for a specific key with optional triggers.
func MockerSet(key string, trigger int) {
	mockerSet(key, "", false, trigger)
}

// MockerPanic sets a mock for a specific key that will panic when triggered.
func MockerPanic(key string, trigger int) {
	mockerSet(key, "", true, trigger)
}

// MockerMatchSet sets a mock for a specific key with a regex match.
func MockerMatchSet(key, match string) {
	mockerSet(key, match, false)
}

// MockerMatchPanic sets a mock for a specific key that will panic when the regex match is triggered.
func MockerMatchPanic(key, match string) {
	mockerSet(key, match, true)
}

// MockerSetCall sets a mock for a specific key with multiple triggers.
func MockerSetCall(args ...any) (caller *MockerCaller) {
	caller = &MockerCaller{}
	caller.Call = func(call func(trigger int) (res xmap.M, err error)) xmap.M {
		rangeArgs(args, func(key string, i int) {
			MockerSet(key, i)
			res, err := call(i)
			MockerClear()
			caller.Shoulder.Valid(6, res, err)
		})
		return nil
	}
	return
}

// MockerPanicCall sets a mock for a specific key that will panic when triggered.
func MockerPanicCall(args ...any) (caller *MockerCaller) {
	caller = &MockerCaller{}
	caller.Call = func(call func(trigger int) (res xmap.M, err error)) xmap.M {
		rangeArgs(args, func(key string, i int) {
			MockerPanic(key, i)
			res, err := call(i)
			MockerClear()
			caller.Shoulder.Valid(6, res, err)
		})
		return nil
	}
	return
}

// MockerMatchSetCall sets a mock for a specific key with a regex match and allows for a call function.
func MockerMatchSetCall(key, match string) (caller *MockerCaller) {
	caller = &MockerCaller{}
	caller.Call = func(call func(trigger int) (res xmap.M, err error)) xmap.M {
		MockerMatchSet(key, match)
		res, err := call(0)
		MockerClear()
		caller.Shoulder.Valid(4, res, err)
		return res
	}
	return
}

// MockerMatchPanicCall sets a mock for a specific key with a regex match that will panic when triggered.
func MockerMatchPanicCall(key, match string) (caller *MockerCaller) {
	caller = &MockerCaller{}
	caller.Call = func(call func(trigger int) (res xmap.M, err error)) xmap.M {
		MockerMatchPanic(key, match)
		res, err := call(0)
		MockerClear()
		caller.Shoulder.Valid(4, res, err)
		return res
	}
	return
}

// MockerSetRangeCall sets a mock for a specific key with a range of triggers.
func MockerSetRangeCall(key string, start, end int) (caller *MockerCaller) {
	caller = &MockerCaller{}
	caller.Call = func(call func(trigger int) (res xmap.M, err error)) xmap.M {
		for i := start; i < end; i++ {
			MockerSet(key, i)
			res, err := call(0)
			MockerClear()
			caller.Shoulder.Valid(4, res, err)
		}
		return nil
	}
	return
}

// MockerPanicRangeCall sets a mock for a specific key that will panic when triggered within a range.
func MockerPanicRangeCall(key string, start, end int) (caller *MockerCaller) {
	caller = &MockerCaller{}
	caller.Call = func(call func(trigger int) (res xmap.M, err error)) xmap.M {
		for i := start; i < end; i++ {
			MockerPanic(key, i)
			res, err := call(0)
			MockerClear()
			caller.Shoulder.Valid(4, res, err)
		}
		return nil
	}
	return
}
