package main

import (
	"testing"
)

func TestGenRandomInt(t *testing.T) {
	for i := 0; i < 100; i++ {
		got := GenRandomInt(2)
		if got < 0 || got > 1 {
			t.Fatalf("Generated number is not between [0, 2], got=%d", got)
		}
	}
}
