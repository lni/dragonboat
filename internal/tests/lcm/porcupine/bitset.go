package porcupine

type bitset []uint64

// data layout:
// bits 0-63 are in data[0], the next are in data[1], etc.

func newBitset(bits uint) bitset {
	extra := uint(0)
	if bits%64 != 0 {
		extra = 1
	}
	chunks := bits/64 + extra
	return bitset(make([]uint64, chunks))
}

func (b bitset) clone() bitset {
	dataCopy := make([]uint64, len(b))
	copy(dataCopy, b)
	return bitset(dataCopy)
}

func bitsetIndex(pos uint) (uint, uint) {
	return pos / 64, pos % 64
}

func (b bitset) set(pos uint) bitset {
	major, minor := bitsetIndex(pos)
	b[major] |= (1 << minor)
	return b
}

func (b bitset) clear(pos uint) bitset {
	major, minor := bitsetIndex(pos)
	b[major] &^= (1 << minor)
	return b
}

func (b bitset) get(pos uint) bool {
	major, minor := bitsetIndex(pos)
	return b[major]&(1<<minor) != 0
}

func (b bitset) popcnt() uint {
	total := uint(0)
	for _, v := range b {
		v = (v & 0x5555555555555555) + ((v & 0xAAAAAAAAAAAAAAAA) >> 1)
		v = (v & 0x3333333333333333) + ((v & 0xCCCCCCCCCCCCCCCC) >> 2)
		v = (v & 0x0F0F0F0F0F0F0F0F) + ((v & 0xF0F0F0F0F0F0F0F0) >> 4)
		v *= 0x0101010101010101
		total += uint((v >> 56) & 0xFF)
	}
	return total
}

func (b bitset) hash() uint64 {
	hash := uint64(b.popcnt())
	for _, v := range b {
		hash ^= v
	}
	return hash
}

func (b bitset) equals(b2 bitset) bool {
	if len(b) != len(b2) {
		return false
	}
	for i := range b {
		if b[i] != b2[i] {
			return false
		}
	}
	return true
}
