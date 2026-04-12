package storage

import (
	"bytes"
	"testing"
)

func TestDocArena_BasicCopy(t *testing.T) {
	var a docArena
	src := []byte("hello world")
	got := a.copyBytes(src)
	if !bytes.Equal(got, src) {
		t.Fatalf("got %q, want %q", got, src)
	}
	// Mutating source must not affect the copy.
	src[0] = 'X'
	if got[0] == 'X' {
		t.Fatal("arena slice aliases source")
	}
}

func TestDocArena_MultipleDocs(t *testing.T) {
	var a docArena
	d1 := a.copyBytes([]byte("aaaa"))
	d2 := a.copyBytes([]byte("bbbb"))
	// d1 and d2 must not alias each other.
	if &d1[0] == &d2[0] {
		t.Fatal("unexpected aliasing")
	}
	if !bytes.Equal(d1, []byte("aaaa")) {
		t.Fatalf("d1 corrupted: %q", d1)
	}
	if !bytes.Equal(d2, []byte("bbbb")) {
		t.Fatalf("d2 corrupted: %q", d2)
	}
	// Writing to d2 must not corrupt d1 (they share a slab but not offsets).
	d2[0] = 'Z'
	if d1[0] == 'Z' {
		t.Fatal("d2 write corrupted d1")
	}
}

func TestDocArena_LargeDocExceedsSlab(t *testing.T) {
	var a docArena
	// Fill most of the first slab.
	small := make([]byte, docArenaSize-10)
	for i := range small {
		small[i] = 'x'
	}
	s1 := a.copyBytes(small)

	// This doc won't fit in the remaining 10 bytes — triggers a new slab.
	big := make([]byte, 100)
	for i := range big {
		big[i] = 'y'
	}
	s2 := a.copyBytes(big)

	if !bytes.Equal(s1, small) {
		t.Fatal("s1 corrupted after slab switch")
	}
	if !bytes.Equal(s2, big) {
		t.Fatal("s2 incorrect")
	}
}

func TestDocArena_OversizedDoc(t *testing.T) {
	var a docArena
	// A single doc bigger than docArenaSize.
	huge := make([]byte, docArenaSize*2)
	for i := range huge {
		huge[i] = 'z'
	}
	got := a.copyBytes(huge)
	if !bytes.Equal(got, huge) {
		t.Fatal("oversized doc copy failed")
	}
}
