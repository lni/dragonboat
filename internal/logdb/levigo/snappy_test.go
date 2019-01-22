package levigo

import "testing"

func TestSnappy(t *testing.T) {
	if !SnappySupported() {
		t.Fatal("snappy not supported")
	}
}
