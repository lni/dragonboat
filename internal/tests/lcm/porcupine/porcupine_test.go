package porcupine

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"testing"
)

func TestRegisterModel(t *testing.T) {
	t.Parallel()
	// inputs
	type registerInput struct {
		op    bool // false = read, true = write
		value int
	}
	// output
	type registerOutput int // we don't care about return value for write
	registerModel := Model{
		Init: func() interface{} { return 0 },
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			st := state.(int)
			inp := input.(registerInput)
			out := output.(int)
			if inp.op == false {
				// read
				return out == st, state
			} else {
				// write
				return true, inp.value
			}
		},
	}

	// examples taken from http://nil.csail.mit.edu/6.824/2017/quizzes/q2-17-ans.pdf
	// section VII

	ops := []Operation{
		{registerInput{true, 100}, 0, 0, 100},
		{registerInput{false, 0}, 25, 100, 75},
		{registerInput{false, 0}, 30, 0, 60},
	}
	res := CheckOperations(registerModel, ops)
	if res != true {
		t.Fatal("expected operations to be linearizable")
	}

	// same example as above, but with Event
	events := []Event{
		{CallEvent, registerInput{true, 100}, 0},
		{CallEvent, registerInput{false, 0}, 1},
		{CallEvent, registerInput{false, 0}, 2},
		{ReturnEvent, 0, 2},
		{ReturnEvent, 100, 1},
		{ReturnEvent, 0, 0},
	}
	res = CheckEvents(registerModel, events)
	if res != true {
		t.Fatal("expected operations to be linearizable")
	}

	ops = []Operation{
		{registerInput{true, 200}, 0, 0, 100},
		{registerInput{false, 0}, 10, 200, 30},
		{registerInput{false, 0}, 40, 0, 90},
	}
	res = CheckOperations(registerModel, ops)
	if res != false {
		t.Fatal("expected operations to not be linearizable")
	}

	// same example as above, but with Event
	events = []Event{
		{CallEvent, registerInput{true, 200}, 0},
		{CallEvent, registerInput{false, 0}, 1},
		{ReturnEvent, 200, 1},
		{CallEvent, registerInput{false, 0}, 2},
		{ReturnEvent, 0, 2},
		{ReturnEvent, 0, 0},
	}
	res = CheckEvents(registerModel, events)
	if res != false {
		t.Fatal("expected operations to not be linearizable")
	}
}

type etcdInput struct {
	op   uint8 // 0 => read, 1 => write, 2 => cas
	arg1 int   // used for write, or for CAS from argument
	arg2 int   // used for CAS to argument
}

type etcdOutput struct {
	ok      bool // used for CAS
	exists  bool // used for read
	value   int  // used for read
	unknown bool // used when operation times out
}

func checkJepsen(t *testing.T, logNum int, correct bool) {
	t.Parallel()
	etcdModel := getEtcdModel()
	events := parseJepsenLog(fmt.Sprintf("test_data/jepsen/etcd_%03d.log", logNum))
	res := CheckEvents(etcdModel, events)
	if res != correct {
		t.Fatalf("expected output %t, got output %t", correct, res)
	}
}

func TestEtcdJepsen000(t *testing.T) {
	checkJepsen(t, 0, false)
}

func TestEtcdJepsen001(t *testing.T) {
	checkJepsen(t, 1, false)
}

func TestEtcdJepsen002(t *testing.T) {
	checkJepsen(t, 2, true)
}

func TestEtcdJepsen003(t *testing.T) {
	checkJepsen(t, 3, false)
}

func TestEtcdJepsen004(t *testing.T) {
	checkJepsen(t, 4, false)
}

func TestEtcdJepsen005(t *testing.T) {
	checkJepsen(t, 5, true)
}

func TestEtcdJepsen006(t *testing.T) {
	checkJepsen(t, 6, false)
}

func TestEtcdJepsen007(t *testing.T) {
	checkJepsen(t, 7, true)
}

func TestEtcdJepsen008(t *testing.T) {
	checkJepsen(t, 8, false)
}

func TestEtcdJepsen009(t *testing.T) {
	checkJepsen(t, 9, false)
}

func TestEtcdJepsen010(t *testing.T) {
	checkJepsen(t, 10, false)
}

func TestEtcdJepsen011(t *testing.T) {
	checkJepsen(t, 11, false)
}

func TestEtcdJepsen012(t *testing.T) {
	checkJepsen(t, 12, false)
}

func TestEtcdJepsen013(t *testing.T) {
	checkJepsen(t, 13, false)
}

func TestEtcdJepsen014(t *testing.T) {
	checkJepsen(t, 14, false)
}

func TestEtcdJepsen015(t *testing.T) {
	checkJepsen(t, 15, false)
}

func TestEtcdJepsen016(t *testing.T) {
	checkJepsen(t, 16, false)
}

func TestEtcdJepsen017(t *testing.T) {
	checkJepsen(t, 17, false)
}

func TestEtcdJepsen018(t *testing.T) {
	checkJepsen(t, 18, true)
}

func TestEtcdJepsen019(t *testing.T) {
	checkJepsen(t, 19, false)
}

func TestEtcdJepsen020(t *testing.T) {
	checkJepsen(t, 20, false)
}

func TestEtcdJepsen021(t *testing.T) {
	checkJepsen(t, 21, false)
}

func TestEtcdJepsen022(t *testing.T) {
	checkJepsen(t, 22, false)
}

func TestEtcdJepsen023(t *testing.T) {
	checkJepsen(t, 23, false)
}

func TestEtcdJepsen024(t *testing.T) {
	checkJepsen(t, 24, false)
}

func TestEtcdJepsen025(t *testing.T) {
	checkJepsen(t, 25, true)
}

func TestEtcdJepsen026(t *testing.T) {
	checkJepsen(t, 26, false)
}

func TestEtcdJepsen027(t *testing.T) {
	checkJepsen(t, 27, false)
}

func TestEtcdJepsen028(t *testing.T) {
	checkJepsen(t, 28, false)
}

func TestEtcdJepsen029(t *testing.T) {
	checkJepsen(t, 29, false)
}

func TestEtcdJepsen030(t *testing.T) {
	checkJepsen(t, 30, false)
}

func TestEtcdJepsen031(t *testing.T) {
	checkJepsen(t, 31, true)
}

func TestEtcdJepsen032(t *testing.T) {
	checkJepsen(t, 32, false)
}

func TestEtcdJepsen033(t *testing.T) {
	checkJepsen(t, 33, false)
}

func TestEtcdJepsen034(t *testing.T) {
	checkJepsen(t, 34, false)
}

func TestEtcdJepsen035(t *testing.T) {
	checkJepsen(t, 35, false)
}

func TestEtcdJepsen036(t *testing.T) {
	checkJepsen(t, 36, false)
}

func TestEtcdJepsen037(t *testing.T) {
	checkJepsen(t, 37, false)
}

func TestEtcdJepsen038(t *testing.T) {
	checkJepsen(t, 38, true)
}

func TestEtcdJepsen039(t *testing.T) {
	checkJepsen(t, 39, false)
}

func TestEtcdJepsen040(t *testing.T) {
	checkJepsen(t, 40, false)
}

func TestEtcdJepsen041(t *testing.T) {
	checkJepsen(t, 41, false)
}

func TestEtcdJepsen042(t *testing.T) {
	checkJepsen(t, 42, false)
}

func TestEtcdJepsen043(t *testing.T) {
	checkJepsen(t, 43, false)
}

func TestEtcdJepsen044(t *testing.T) {
	checkJepsen(t, 44, false)
}

func TestEtcdJepsen045(t *testing.T) {
	checkJepsen(t, 45, true)
}

func TestEtcdJepsen046(t *testing.T) {
	checkJepsen(t, 46, false)
}

func TestEtcdJepsen047(t *testing.T) {
	checkJepsen(t, 47, false)
}

func TestEtcdJepsen048(t *testing.T) {
	checkJepsen(t, 48, true)
}

func TestEtcdJepsen049(t *testing.T) {
	checkJepsen(t, 49, true)
}

func TestEtcdJepsen050(t *testing.T) {
	checkJepsen(t, 50, false)
}

func TestEtcdJepsen051(t *testing.T) {
	checkJepsen(t, 51, true)
}

func TestEtcdJepsen052(t *testing.T) {
	checkJepsen(t, 52, false)
}

func TestEtcdJepsen053(t *testing.T) {
	checkJepsen(t, 53, true)
}

func TestEtcdJepsen054(t *testing.T) {
	checkJepsen(t, 54, false)
}

func TestEtcdJepsen055(t *testing.T) {
	checkJepsen(t, 55, false)
}

func TestEtcdJepsen056(t *testing.T) {
	checkJepsen(t, 56, true)
}

func TestEtcdJepsen057(t *testing.T) {
	checkJepsen(t, 57, false)
}

func TestEtcdJepsen058(t *testing.T) {
	checkJepsen(t, 58, false)
}

func TestEtcdJepsen059(t *testing.T) {
	checkJepsen(t, 59, false)
}

func TestEtcdJepsen060(t *testing.T) {
	checkJepsen(t, 60, false)
}

func TestEtcdJepsen061(t *testing.T) {
	checkJepsen(t, 61, false)
}

func TestEtcdJepsen062(t *testing.T) {
	checkJepsen(t, 62, false)
}

func TestEtcdJepsen063(t *testing.T) {
	checkJepsen(t, 63, false)
}

func TestEtcdJepsen064(t *testing.T) {
	checkJepsen(t, 64, false)
}

func TestEtcdJepsen065(t *testing.T) {
	checkJepsen(t, 65, false)
}

func TestEtcdJepsen066(t *testing.T) {
	checkJepsen(t, 66, false)
}

func TestEtcdJepsen067(t *testing.T) {
	checkJepsen(t, 67, true)
}

func TestEtcdJepsen068(t *testing.T) {
	checkJepsen(t, 68, false)
}

func TestEtcdJepsen069(t *testing.T) {
	checkJepsen(t, 69, false)
}

func TestEtcdJepsen070(t *testing.T) {
	checkJepsen(t, 70, false)
}

func TestEtcdJepsen071(t *testing.T) {
	checkJepsen(t, 71, false)
}

func TestEtcdJepsen072(t *testing.T) {
	checkJepsen(t, 72, false)
}

func TestEtcdJepsen073(t *testing.T) {
	checkJepsen(t, 73, false)
}

func TestEtcdJepsen074(t *testing.T) {
	checkJepsen(t, 74, false)
}

func TestEtcdJepsen075(t *testing.T) {
	checkJepsen(t, 75, true)
}

func TestEtcdJepsen076(t *testing.T) {
	checkJepsen(t, 76, true)
}

func TestEtcdJepsen077(t *testing.T) {
	checkJepsen(t, 77, false)
}

func TestEtcdJepsen078(t *testing.T) {
	checkJepsen(t, 78, false)
}

func TestEtcdJepsen079(t *testing.T) {
	checkJepsen(t, 79, false)
}

func TestEtcdJepsen080(t *testing.T) {
	checkJepsen(t, 80, true)
}

func TestEtcdJepsen081(t *testing.T) {
	checkJepsen(t, 81, false)
}

func TestEtcdJepsen082(t *testing.T) {
	checkJepsen(t, 82, false)
}

func TestEtcdJepsen083(t *testing.T) {
	checkJepsen(t, 83, false)
}

func TestEtcdJepsen084(t *testing.T) {
	checkJepsen(t, 84, false)
}

func TestEtcdJepsen085(t *testing.T) {
	checkJepsen(t, 85, false)
}

func TestEtcdJepsen086(t *testing.T) {
	checkJepsen(t, 86, false)
}

func TestEtcdJepsen087(t *testing.T) {
	checkJepsen(t, 87, true)
}

func TestEtcdJepsen088(t *testing.T) {
	checkJepsen(t, 88, false)
}

func TestEtcdJepsen089(t *testing.T) {
	checkJepsen(t, 89, false)
}

func TestEtcdJepsen090(t *testing.T) {
	checkJepsen(t, 90, false)
}

func TestEtcdJepsen091(t *testing.T) {
	checkJepsen(t, 91, false)
}

func TestEtcdJepsen092(t *testing.T) {
	checkJepsen(t, 92, true)
}

func TestEtcdJepsen093(t *testing.T) {
	checkJepsen(t, 93, false)
}

func TestEtcdJepsen094(t *testing.T) {
	checkJepsen(t, 94, false)
}

// etcd cluster failed to start up in test 95

func TestEtcdJepsen096(t *testing.T) {
	checkJepsen(t, 96, false)
}

func TestEtcdJepsen097(t *testing.T) {
	checkJepsen(t, 97, false)
}

func TestEtcdJepsen098(t *testing.T) {
	checkJepsen(t, 98, true)
}

func TestEtcdJepsen099(t *testing.T) {
	checkJepsen(t, 99, false)
}

func TestEtcdJepsen100(t *testing.T) {
	checkJepsen(t, 100, true)
}

func TestEtcdJepsen101(t *testing.T) {
	checkJepsen(t, 101, true)
}

func TestEtcdJepsen102(t *testing.T) {
	checkJepsen(t, 102, true)
}

func benchJepsen(b *testing.B, logNum int, correct bool) {
	etcdModel := getEtcdModel()
	events := parseJepsenLog(fmt.Sprintf("test_data/jepsen/etcd_%03d.log", logNum))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := CheckEvents(etcdModel, events)
		if res != correct {
			b.Fatalf("expected output %t, got output %t", correct, res)
		}
	}
}

func BenchmarkEtcdJepsen000(b *testing.B) {
	benchJepsen(b, 0, false)
}

func BenchmarkEtcdJepsen001(b *testing.B) {
	benchJepsen(b, 1, false)
}

func BenchmarkEtcdJepsen002(b *testing.B) {
	benchJepsen(b, 2, true)
}

func BenchmarkEtcdJepsen003(b *testing.B) {
	benchJepsen(b, 3, false)
}

func BenchmarkEtcdJepsen004(b *testing.B) {
	benchJepsen(b, 4, false)
}

func BenchmarkEtcdJepsen005(b *testing.B) {
	benchJepsen(b, 5, true)
}

func BenchmarkEtcdJepsen006(b *testing.B) {
	benchJepsen(b, 6, false)
}

func BenchmarkEtcdJepsen007(b *testing.B) {
	benchJepsen(b, 7, true)
}

func BenchmarkEtcdJepsen008(b *testing.B) {
	benchJepsen(b, 8, false)
}

func BenchmarkEtcdJepsen009(b *testing.B) {
	benchJepsen(b, 9, false)
}

func BenchmarkEtcdJepsen010(b *testing.B) {
	benchJepsen(b, 10, false)
}

func BenchmarkEtcdJepsen011(b *testing.B) {
	benchJepsen(b, 11, false)
}

func BenchmarkEtcdJepsen012(b *testing.B) {
	benchJepsen(b, 12, false)
}

func BenchmarkEtcdJepsen013(b *testing.B) {
	benchJepsen(b, 13, false)
}

func BenchmarkEtcdJepsen014(b *testing.B) {
	benchJepsen(b, 14, false)
}

func BenchmarkEtcdJepsen015(b *testing.B) {
	benchJepsen(b, 15, false)
}

func BenchmarkEtcdJepsen016(b *testing.B) {
	benchJepsen(b, 16, false)
}

func BenchmarkEtcdJepsen017(b *testing.B) {
	benchJepsen(b, 17, false)
}

func BenchmarkEtcdJepsen018(b *testing.B) {
	benchJepsen(b, 18, true)
}

func BenchmarkEtcdJepsen019(b *testing.B) {
	benchJepsen(b, 19, false)
}

func BenchmarkEtcdJepsen020(b *testing.B) {
	benchJepsen(b, 20, false)
}

func BenchmarkEtcdJepsen021(b *testing.B) {
	benchJepsen(b, 21, false)
}

func BenchmarkEtcdJepsen022(b *testing.B) {
	benchJepsen(b, 22, false)
}

func BenchmarkEtcdJepsen023(b *testing.B) {
	benchJepsen(b, 23, false)
}

func BenchmarkEtcdJepsen024(b *testing.B) {
	benchJepsen(b, 24, false)
}

func BenchmarkEtcdJepsen025(b *testing.B) {
	benchJepsen(b, 25, true)
}

func BenchmarkEtcdJepsen026(b *testing.B) {
	benchJepsen(b, 26, false)
}

func BenchmarkEtcdJepsen027(b *testing.B) {
	benchJepsen(b, 27, false)
}

func BenchmarkEtcdJepsen028(b *testing.B) {
	benchJepsen(b, 28, false)
}

func BenchmarkEtcdJepsen029(b *testing.B) {
	benchJepsen(b, 29, false)
}

func BenchmarkEtcdJepsen030(b *testing.B) {
	benchJepsen(b, 30, false)
}

func BenchmarkEtcdJepsen031(b *testing.B) {
	benchJepsen(b, 31, true)
}

func BenchmarkEtcdJepsen032(b *testing.B) {
	benchJepsen(b, 32, false)
}

func BenchmarkEtcdJepsen033(b *testing.B) {
	benchJepsen(b, 33, false)
}

func BenchmarkEtcdJepsen034(b *testing.B) {
	benchJepsen(b, 34, false)
}

func BenchmarkEtcdJepsen035(b *testing.B) {
	benchJepsen(b, 35, false)
}

func BenchmarkEtcdJepsen036(b *testing.B) {
	benchJepsen(b, 36, false)
}

func BenchmarkEtcdJepsen037(b *testing.B) {
	benchJepsen(b, 37, false)
}

func BenchmarkEtcdJepsen038(b *testing.B) {
	benchJepsen(b, 38, true)
}

func BenchmarkEtcdJepsen039(b *testing.B) {
	benchJepsen(b, 39, false)
}

func BenchmarkEtcdJepsen040(b *testing.B) {
	benchJepsen(b, 40, false)
}

func BenchmarkEtcdJepsen041(b *testing.B) {
	benchJepsen(b, 41, false)
}

func BenchmarkEtcdJepsen042(b *testing.B) {
	benchJepsen(b, 42, false)
}

func BenchmarkEtcdJepsen043(b *testing.B) {
	benchJepsen(b, 43, false)
}

func BenchmarkEtcdJepsen044(b *testing.B) {
	benchJepsen(b, 44, false)
}

func BenchmarkEtcdJepsen045(b *testing.B) {
	benchJepsen(b, 45, true)
}

func BenchmarkEtcdJepsen046(b *testing.B) {
	benchJepsen(b, 46, false)
}

func BenchmarkEtcdJepsen047(b *testing.B) {
	benchJepsen(b, 47, false)
}

func BenchmarkEtcdJepsen048(b *testing.B) {
	benchJepsen(b, 48, true)
}

func BenchmarkEtcdJepsen049(b *testing.B) {
	benchJepsen(b, 49, true)
}

func BenchmarkEtcdJepsen050(b *testing.B) {
	benchJepsen(b, 50, false)
}

func BenchmarkEtcdJepsen051(b *testing.B) {
	benchJepsen(b, 51, true)
}

func BenchmarkEtcdJepsen052(b *testing.B) {
	benchJepsen(b, 52, false)
}

func BenchmarkEtcdJepsen053(b *testing.B) {
	benchJepsen(b, 53, true)
}

func BenchmarkEtcdJepsen054(b *testing.B) {
	benchJepsen(b, 54, false)
}

func BenchmarkEtcdJepsen055(b *testing.B) {
	benchJepsen(b, 55, false)
}

func BenchmarkEtcdJepsen056(b *testing.B) {
	benchJepsen(b, 56, true)
}

func BenchmarkEtcdJepsen057(b *testing.B) {
	benchJepsen(b, 57, false)
}

func BenchmarkEtcdJepsen058(b *testing.B) {
	benchJepsen(b, 58, false)
}

func BenchmarkEtcdJepsen059(b *testing.B) {
	benchJepsen(b, 59, false)
}

func BenchmarkEtcdJepsen060(b *testing.B) {
	benchJepsen(b, 60, false)
}

func BenchmarkEtcdJepsen061(b *testing.B) {
	benchJepsen(b, 61, false)
}

func BenchmarkEtcdJepsen062(b *testing.B) {
	benchJepsen(b, 62, false)
}

func BenchmarkEtcdJepsen063(b *testing.B) {
	benchJepsen(b, 63, false)
}

func BenchmarkEtcdJepsen064(b *testing.B) {
	benchJepsen(b, 64, false)
}

func BenchmarkEtcdJepsen065(b *testing.B) {
	benchJepsen(b, 65, false)
}

func BenchmarkEtcdJepsen066(b *testing.B) {
	benchJepsen(b, 66, false)
}

func BenchmarkEtcdJepsen067(b *testing.B) {
	benchJepsen(b, 67, true)
}

func BenchmarkEtcdJepsen068(b *testing.B) {
	benchJepsen(b, 68, false)
}

func BenchmarkEtcdJepsen069(b *testing.B) {
	benchJepsen(b, 69, false)
}

func BenchmarkEtcdJepsen070(b *testing.B) {
	benchJepsen(b, 70, false)
}

func BenchmarkEtcdJepsen071(b *testing.B) {
	benchJepsen(b, 71, false)
}

func BenchmarkEtcdJepsen072(b *testing.B) {
	benchJepsen(b, 72, false)
}

func BenchmarkEtcdJepsen073(b *testing.B) {
	benchJepsen(b, 73, false)
}

func BenchmarkEtcdJepsen074(b *testing.B) {
	benchJepsen(b, 74, false)
}

func BenchmarkEtcdJepsen075(b *testing.B) {
	benchJepsen(b, 75, true)
}

func BenchmarkEtcdJepsen076(b *testing.B) {
	benchJepsen(b, 76, true)
}

func BenchmarkEtcdJepsen077(b *testing.B) {
	benchJepsen(b, 77, false)
}

func BenchmarkEtcdJepsen078(b *testing.B) {
	benchJepsen(b, 78, false)
}

func BenchmarkEtcdJepsen079(b *testing.B) {
	benchJepsen(b, 79, false)
}

func BenchmarkEtcdJepsen080(b *testing.B) {
	benchJepsen(b, 80, true)
}

func BenchmarkEtcdJepsen081(b *testing.B) {
	benchJepsen(b, 81, false)
}

func BenchmarkEtcdJepsen082(b *testing.B) {
	benchJepsen(b, 82, false)
}

func BenchmarkEtcdJepsen083(b *testing.B) {
	benchJepsen(b, 83, false)
}

func BenchmarkEtcdJepsen084(b *testing.B) {
	benchJepsen(b, 84, false)
}

func BenchmarkEtcdJepsen085(b *testing.B) {
	benchJepsen(b, 85, false)
}

func BenchmarkEtcdJepsen086(b *testing.B) {
	benchJepsen(b, 86, false)
}

func BenchmarkEtcdJepsen087(b *testing.B) {
	benchJepsen(b, 87, true)
}

func BenchmarkEtcdJepsen088(b *testing.B) {
	benchJepsen(b, 88, false)
}

func BenchmarkEtcdJepsen089(b *testing.B) {
	benchJepsen(b, 89, false)
}

func BenchmarkEtcdJepsen090(b *testing.B) {
	benchJepsen(b, 90, false)
}

func BenchmarkEtcdJepsen091(b *testing.B) {
	benchJepsen(b, 91, false)
}

func BenchmarkEtcdJepsen092(b *testing.B) {
	benchJepsen(b, 92, true)
}

func BenchmarkEtcdJepsen093(b *testing.B) {
	benchJepsen(b, 93, false)
}

func BenchmarkEtcdJepsen094(b *testing.B) {
	benchJepsen(b, 94, false)
}

// etcd cluster failed to start up in test 95

func BenchmarkEtcdJepsen096(b *testing.B) {
	benchJepsen(b, 96, false)
}

func BenchmarkEtcdJepsen097(b *testing.B) {
	benchJepsen(b, 97, false)
}

func BenchmarkEtcdJepsen098(b *testing.B) {
	benchJepsen(b, 98, true)
}

func BenchmarkEtcdJepsen099(b *testing.B) {
	benchJepsen(b, 99, false)
}

func BenchmarkEtcdJepsen100(b *testing.B) {
	benchJepsen(b, 100, true)
}

func BenchmarkEtcdJepsen101(b *testing.B) {
	benchJepsen(b, 101, true)
}

func BenchmarkEtcdJepsen102(b *testing.B) {
	benchJepsen(b, 102, true)
}

type kvInput struct {
	op    uint8 // 0 => get, 1 => put, 2 => append
	key   string
	value string
}

type kvOutput struct {
	value string
}

func getKvModel() Model {
	return Model{
		PartitionEvent: func(history []Event) [][]Event {
			m := make(map[string][]Event)
			match := make(map[uint]string) // id -> key
			for _, v := range history {
				if v.Kind == CallEvent {
					key := v.Value.(kvInput).key
					m[key] = append(m[key], v)
					match[v.Id] = key
				} else {
					key := match[v.Id]
					m[key] = append(m[key], v)
				}
			}
			var ret [][]Event
			for _, v := range m {
				ret = append(ret, v)
			}
			return ret
		},
		Init: func() interface{} {
			// note: we are modeling a single key's value here;
			// we're partitioning by key, so this is okay
			return ""
		},
		Step: func(state, input, output interface{}) (bool, interface{}) {
			inp := input.(kvInput)
			out := output.(kvOutput)
			st := state.(string)
			if inp.op == 0 {
				// get
				return out.value == st, state
			} else if inp.op == 1 {
				// put
				return true, inp.value
			} else {
				// append
				return true, (st + inp.value)
			}
		},
	}
}

func parseKvLog(filename string) []Event {
	file, err := os.Open(filename)
	if err != nil {
		panic("can't open file")
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	invokeGet, _ := regexp.Compile(`{:process (\d+), :type :invoke, :f :get, :key "(.*)", :value nil}`)
	invokePut, _ := regexp.Compile(`{:process (\d+), :type :invoke, :f :put, :key "(.*)", :value "(.*)"}`)
	invokeAppend, _ := regexp.Compile(`{:process (\d+), :type :invoke, :f :append, :key "(.*)", :value "(.*)"}`)
	returnGet, _ := regexp.Compile(`{:process (\d+), :type :ok, :f :get, :key ".*", :value "(.*)"}`)
	returnPut, _ := regexp.Compile(`{:process (\d+), :type :ok, :f :put, :key ".*", :value ".*"}`)
	returnAppend, _ := regexp.Compile(`{:process (\d+), :type :ok, :f :append, :key ".*", :value ".*"}`)

	var events []Event = nil

	id := uint(0)
	procIdMap := make(map[int]uint)
	for {
		lineBytes, isPrefix, err := reader.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			panic("error while reading file: " + err.Error())
		}
		if isPrefix {
			panic("can't handle isPrefix")
		}
		line := string(lineBytes)

		switch {
		case invokeGet.MatchString(line):
			args := invokeGet.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			events = append(events, Event{CallEvent, kvInput{op: 0, key: args[2]}, id})
			procIdMap[proc] = id
			id++
		case invokePut.MatchString(line):
			args := invokePut.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			events = append(events, Event{CallEvent, kvInput{op: 1, key: args[2], value: args[3]}, id})
			procIdMap[proc] = id
			id++
		case invokeAppend.MatchString(line):
			args := invokeAppend.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			events = append(events, Event{CallEvent, kvInput{op: 2, key: args[2], value: args[3]}, id})
			procIdMap[proc] = id
			id++
		case returnGet.MatchString(line):
			args := returnGet.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			matchId := procIdMap[proc]
			delete(procIdMap, proc)
			events = append(events, Event{ReturnEvent, kvOutput{args[2]}, matchId})
		case returnPut.MatchString(line):
			args := returnPut.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			matchId := procIdMap[proc]
			delete(procIdMap, proc)
			events = append(events, Event{ReturnEvent, kvOutput{}, matchId})
		case returnAppend.MatchString(line):
			args := returnAppend.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			matchId := procIdMap[proc]
			delete(procIdMap, proc)
			events = append(events, Event{ReturnEvent, kvOutput{}, matchId})
		}
	}

	for _, matchId := range procIdMap {
		events = append(events, Event{ReturnEvent, kvOutput{}, matchId})
	}

	return events
}

func checkKv(t *testing.T, logName string, correct bool) {
	t.Parallel()
	kvModel := getKvModel()
	events := parseKvLog(fmt.Sprintf("test_data/kv/%s.txt", logName))
	res := CheckEvents(kvModel, events)
	if res != correct {
		t.Fatalf("expected output %t, got output %t", correct, res)
	}
}

func TestKv1ClientOk(t *testing.T) {
	checkKv(t, "c01-ok", true)
}

func TestKv1ClientBad(t *testing.T) {
	checkKv(t, "c01-bad", false)
}

func TestKv10ClientsOk(t *testing.T) {
	checkKv(t, "c10-ok", true)
}

func TestKv10ClientsBad(t *testing.T) {
	checkKv(t, "c10-bad", false)
}

func TestKv50ClientsOk(t *testing.T) {
	checkKv(t, "c50-ok", true)
}

func TestKv50ClientsBad(t *testing.T) {
	checkKv(t, "c50-bad", false)
}

func TestSetModel(t *testing.T) {
	t.Parallel()

	// Set Model is from Jepsen/Knossos Set.
	// A set supports add and read operations, and we must ensure that
	// each read can't read duplicated or unknown values from the set

	// inputs
	type setInput struct {
		op    bool // false = read, true = write
		value int
	}

	// outputs
	type setOutput struct {
		values  []int // read
		unknown bool  // read
	}

	setModel := Model{
		Init: func() interface{} { return []int{} },
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			st := state.([]int)
			inp := input.(setInput)
			out := output.(setOutput)

			if inp.op == true {
				// always returns true for write
				index := sort.SearchInts(st, inp.value)
				if index >= len(st) || st[index] != inp.value {
					// value not in the set
					st = append(st, inp.value)
					sort.Ints(st)
				}
				return true, st
			}

			sort.Ints(out.values)
			return out.unknown || reflect.DeepEqual(st, out.values), out.values
		},
		Equal: func(state1, state2 interface{}) bool {
			return reflect.DeepEqual(state1, state2)
		},
	}

	events := []Event{
		{CallEvent, setInput{true, 100}, 0},
		{CallEvent, setInput{true, 0}, 1},
		{CallEvent, setInput{false, 0}, 2},
		{ReturnEvent, setOutput{[]int{100}, false}, 2},
		{ReturnEvent, setOutput{}, 1},
		{ReturnEvent, setOutput{}, 0},
	}
	res := CheckEvents(setModel, events)
	if res != true {
		t.Fatal("expected operations to be linearizable")
	}

	events = []Event{
		{CallEvent, setInput{true, 100}, 0},
		{CallEvent, setInput{true, 110}, 1},
		{CallEvent, setInput{false, 0}, 2},
		{ReturnEvent, setOutput{[]int{100, 110}, false}, 2},
		{ReturnEvent, setOutput{}, 1},
		{ReturnEvent, setOutput{}, 0},
	}
	res = CheckEvents(setModel, events)
	if res != true {
		t.Fatal("expected operations to be linearizable")
	}

	events = []Event{
		{CallEvent, setInput{true, 100}, 0},
		{CallEvent, setInput{true, 110}, 1},
		{CallEvent, setInput{false, 0}, 2},
		{ReturnEvent, setOutput{[]int{}, true}, 2},
		{ReturnEvent, setOutput{}, 1},
		{ReturnEvent, setOutput{}, 0},
	}
	res = CheckEvents(setModel, events)
	if res != true {
		t.Fatal("expected operations to be linearizable")
	}

	events = []Event{
		{CallEvent, setInput{true, 100}, 0},
		{CallEvent, setInput{true, 110}, 1},
		{CallEvent, setInput{false, 0}, 2},
		{ReturnEvent, setOutput{[]int{100, 100, 110}, false}, 2},
		{ReturnEvent, setOutput{}, 1},
		{ReturnEvent, setOutput{}, 0},
	}
	res = CheckEvents(setModel, events)
	if res == true {
		t.Fatal("expected operations not to be linearizable")
	}
}
