package miniflow

import (
	"reflect"
	"runtime/debug"
	"testing"
)

func assert(t *testing.T, exp, got interface{}, equal bool) {
	if reflect.DeepEqual(exp, got) != equal {
		debug.PrintStack()
		t.Fatalf("Expecting '%v' got '%v'\n", exp, got)
	}
}

func assertTrue(t *testing.T, got interface{}) {
	if reflect.DeepEqual(true, got) != true {
		debug.PrintStack()
		t.Fatalf("Expecting true got '%v'\n", got)
	}
}

func assertFalse(t *testing.T, got interface{}) {
	if reflect.DeepEqual(false, got) != true {
		debug.PrintStack()
		t.Fatalf("Expecting false got '%v'\n", got)
	}
}

func assertEqual(t *testing.T, exp, got interface{}) {
	if reflect.DeepEqual(exp, got) != true {
		debug.PrintStack()
		t.Fatalf("Expecting '%v' got '%v'\n", exp, got)
	}
}

func assertNotEqual(t *testing.T, exp, got interface{}) {
	if reflect.DeepEqual(exp, got) != false {
		debug.PrintStack()
		t.Fatalf("Expecting '%v' got '%v'\n", exp, got)
	}
}

func assertNil(t *testing.T, got interface{}) {
	if reflect.DeepEqual(nil, got) != true {
		debug.PrintStack()
		t.Fatalf("Expecting <nil> got '%v'\n", got)
	}
}

func assertNotNil(t *testing.T, got interface{}) {
	if reflect.DeepEqual(nil, got) != false {
		debug.PrintStack()
		t.Fatalf("Expecting not nil got '%v'\n", got)
	}
}
