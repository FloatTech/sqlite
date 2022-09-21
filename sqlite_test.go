package sql

import (
	"bytes"
	"testing"
	"time"
)

func TestPackUnpack(t *testing.T) {
	type teststruct struct {
		A bool
		B int8
		C uint8
		D uint16
		E int32
		F uint32
		G int64
		H uint64
		I float32
		J float64
		K []byte
		L string
		M []string
	}
	db := Sqlite{DBPath: "test.db"}
	err := db.Open(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Create("test", &teststruct{})
	if err != nil {
		t.Fatal(err)
	}
	inst := teststruct{true, 2, 3, 4, 5, 6, 7, 8, 9.0, 10.0, []byte{1, 2, 3}, "123", []string{"123", "456"}}
	err = db.Insert("test", &inst)
	if err != nil {
		t.Fatal(err)
	}
	tmp := teststruct{}
	err = db.Find("test", &tmp, "")
	if err != nil {
		t.Fatal(err)
	}
	if tmp.A != inst.A {
		t.Fail()
	}
	if tmp.B != inst.B {
		t.Fail()
	}
	if tmp.C != inst.C {
		t.Fail()
	}
	if tmp.D != inst.D {
		t.Fail()
	}
	if tmp.E != inst.E {
		t.Fail()
	}
	if tmp.F != inst.F {
		t.Fail()
	}
	if tmp.F != inst.F {
		t.Fail()
	}
	if tmp.G != inst.G {
		t.Fail()
	}
	if tmp.H != inst.H {
		t.Fail()
	}
	if tmp.I != inst.I {
		t.Fail()
	}
	if tmp.J != inst.J {
		t.Fail()
	}
	if !bytes.Equal(tmp.K, inst.K) {
		t.Fail()
	}
	if tmp.L != inst.L {
		t.Fail()
	}
	if tmp.M[0] != inst.M[0] {
		t.Fail()
	}
}
