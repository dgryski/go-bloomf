// Package bloomf implements a simple Bloom Filter for byte slices.
/*
A Bloom Filter is a probabilistic data structure which allows testing set
membership.  A negative answer means the value is not in the set.  A positive
answer means the element is probably is the set.  The desired rate false
positives can be set at filter construction time.

Copyright (c) 2015 Damian Gryski <damian@gryski.com>

Licensed under the MIT License.
*/
package bloomf

import (
	"math"
)

// Internal struct for our bloom filter
type BF struct {
	m      uint32    // size of bit vector in bits
	n      int       // capacity of the bloom filter
	k      uint32    // distinct hash functions needed
	count  int       // number of elements which have been inserted
	filter bitvector // our filter bit vector
	hash   func([]byte) uint64
}

func (bf *BF) Count() int { return bf.count }

// filterBits returns the number of bits required for the desired capacity and
// false positive rate.
func FilterBits(capacity int, falsePositiveRate float64) uint32 {
	bits := float64(capacity) * -math.Log(falsePositiveRate) / (math.Log(2.0) * math.Log(2.0)) // in bits
	m := nextPowerOfTwo(uint32(bits))

	if m < 1024 {
		return 1024
	}

	return m
}

// New returns a new bloom filter with the specified capacity and false positive rate.
func New(capacity int, falsePositiveRate float64, hasher func([]byte) uint64) *BF {

	m := FilterBits(capacity, falsePositiveRate)

	k := uint32(0.7 * float64(m) / float64(capacity))
	if k < 2 {
		k = 2
	}

	return &BF{
		m:      m,
		n:      capacity,
		filter: newbv(m),
		hash:   hasher,
		k:      k,
	}
}

func (bf *BF) Len() int { return bf.count }
func (bf *BF) Cap() int { return bf.n }

// Insert inserts the byte array b into the bloom filter.  Returns true if the value
// was already considered to be in the bloom filter.  Increments if the count if it was not.
func (bf *BF) Insert(b []byte) bool {
	h := bf.hash(b)
	h1, h2 := uint32(h), uint32(h>>32)
	var o uint = 1
	for i := uint32(0); i < bf.k; i++ {
		o &= bf.filter.getset((h1 + (i * h2)) & (bf.m - 1))
	}
	if o == 1 {
		return true
	}
	bf.count++
	return false
}

// Lookup checks the bloom filter for the byte array b
func (bf *BF) Lookup(b []byte) bool {

	h := bf.hash(b)

	h1, h2 := uint32(h), uint32(h>>32)

	for i := uint32(0); i < bf.k; i++ {
		if bf.filter.get((h1+(i*h2))&(bf.m-1)) == 0 {
			return false
		}
	}

	return true
}

// Merge adds bf2 into the current bloom filter.  They must have the same dimensions and use the same hash function.
func (bf *BF) Merge(bf2 BF) {
	// TODO(dgryski): verify parameters match
	for i, v := range bf2.filter {
		bf.filter[i] |= v
	}
}

// Compress halves the space used by the bloom filter, at the cost of increased error rate.
func (bf *BF) Compress() {

	w := len(bf.filter)

	if w&(w-1) != 0 {
		panic("width must be a power of two")
	}

	neww := w / 2

	// We allocate a new array here so old space can actually be garbage collected.
	// TODO(dgryski): reslice and only reallocate every few compressions
	row := make([]uint64, neww)
	for j := 0; j < neww; j++ {
		row[j] = bf.filter[j] | bf.filter[j+neww]
	}
	bf.filter = row
}

// Reset clears the bloom filter
func (bf *BF) Reset() {
	for i := range bf.filter {
		bf.filter[i] = 0
	}
	bf.count = 0
}

// Internal routines for the bit vector
type bitvector []uint64

func newbv(size uint32) bitvector {
	return make([]uint64, uint(size+63)/64)
}

// get bit 'bit' in the bitvector d
func (b bitvector) get(bit uint32) uint {
	shift := bit % 64
	bb := b[bit/64]
	bb &= (1 << shift)

	return uint(bb >> shift)
}

// set bit 'bit' in the bitvector d
func (b bitvector) set(bit uint32) {
	b[bit/64] |= (1 << (bit % 64))
}

// set bit 'bit' in the bitvector d and return previous value
func (b bitvector) getset(bit uint32) uint {
	shift := bit % 64
	idx := bit / 64
	bb := b[idx]
	m := uint64(1) << shift
	b[idx] |= m
	return uint((bb & m) >> shift)
}

// return the integer >= i which is a power of two
func nextPowerOfTwo(i uint32) uint32 {
	n := i - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}
