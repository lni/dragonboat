package porcupine

import (
	"bufio"
	"io"
	"os"
	"regexp"
	"strconv"
)

// GetEtcdModel ...
func GetEtcdModel() Model {
	return getEtcdModel()
}

// ParseJepsenLog ...
func ParseJepsenLog(fn string) []Event {
	return parseJepsenLog(fn)
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

func getEtcdModel() Model {
	return Model{
		Init: func() interface{} { return -1000000 }, // -1000000 corresponds with nil
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			st := state.(int)
			inp := input.(etcdInput)
			out := output.(etcdOutput)
			if inp.op == 0 {
				// read
				ok := (out.exists == false && st == -1000000) || (out.exists == true && st == out.value) || out.unknown
				return ok, state
			} else if inp.op == 1 {
				// write
				return true, inp.arg1
			} else {
				// cas
				ok := (inp.arg1 == st && out.ok) || (inp.arg1 != st && !out.ok) || out.unknown
				result := st
				if inp.arg1 == st {
					result = inp.arg2
				}
				return ok, result
			}
		},
	}
}

func parseJepsenLog(filename string) []Event {
	file, err := os.Open(filename)
	if err != nil {
		panic("can't open file")
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	invokeRead, _ := regexp.Compile(`^INFO\s+jepsen\.util\s+-\s+(\d+)\s+:invoke\s+:read\s+nil$`)
	invokeWrite, _ := regexp.Compile(`^INFO\s+jepsen\.util\s+-\s+(\d+)\s+:invoke\s+:write\s+(\d+)$`)
	invokeCas, _ := regexp.Compile(`^INFO\s+jepsen\.util\s+-\s+(\d+)\s+:invoke\s+:cas\s+\[(\d+)\s+(\d+)\]$`)
	returnRead, _ := regexp.Compile(`^INFO\s+jepsen\.util\s+-\s+(\d+)\s+:ok\s+:read\s+(nil|\d+)$`)
	returnWrite, _ := regexp.Compile(`^INFO\s+jepsen\.util\s+-\s+(\d+)\s+:ok\s+:write\s+(\d+)$`)
	returnCas, _ := regexp.Compile(`^INFO\s+jepsen\.util\s+-\s+(\d+)\s+:(ok|fail)\s+:cas\s+\[(\d+)\s+(\d+)\]$`)
	timeoutRead, _ := regexp.Compile(`^INFO\s+jepsen\.util\s+-\s+(\d+)\s+:fail\s+:read\s+:timed-out$`)

	var events []Event

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
		case invokeRead.MatchString(line):
			args := invokeRead.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			events = append(events, Event{CallEvent, etcdInput{op: 0}, id})
			procIdMap[proc] = id
			id++
		case invokeWrite.MatchString(line):
			args := invokeWrite.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			value, _ := strconv.Atoi(args[2])
			events = append(events, Event{CallEvent, etcdInput{op: 1, arg1: value}, id})
			procIdMap[proc] = id
			id++
		case invokeCas.MatchString(line):
			args := invokeCas.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			from, _ := strconv.Atoi(args[2])
			to, _ := strconv.Atoi(args[3])
			events = append(events, Event{CallEvent, etcdInput{op: 2, arg1: from, arg2: to}, id})
			procIdMap[proc] = id
			id++
		case returnRead.MatchString(line):
			args := returnRead.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			exists := false
			value := 0
			if args[2] != "nil" {
				exists = true
				value, _ = strconv.Atoi(args[2])
			}
			matchId := procIdMap[proc]
			delete(procIdMap, proc)
			events = append(events, Event{ReturnEvent, etcdOutput{exists: exists, value: value}, matchId})
		case returnWrite.MatchString(line):
			args := returnWrite.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			matchId := procIdMap[proc]
			delete(procIdMap, proc)
			events = append(events, Event{ReturnEvent, etcdOutput{}, matchId})
		case returnCas.MatchString(line):
			args := returnCas.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			matchId := procIdMap[proc]
			delete(procIdMap, proc)
			events = append(events, Event{ReturnEvent, etcdOutput{ok: args[2] == "ok"}, matchId})
		case timeoutRead.MatchString(line):
			// timing out a read and then continuing operations is fine
			// we could just delete the read from the events, but we do this the lazy way
			args := timeoutRead.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			matchId := procIdMap[proc]
			delete(procIdMap, proc)
			// okay to put the return here in the history
			events = append(events, Event{ReturnEvent, etcdOutput{unknown: true}, matchId})
		}
	}

	for _, matchId := range procIdMap {
		events = append(events, Event{ReturnEvent, etcdOutput{unknown: true}, matchId})
	}

	return events
}
