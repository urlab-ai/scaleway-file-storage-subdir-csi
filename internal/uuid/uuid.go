package uuid

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// Generator returns a new operation ID.
type Generator interface {
	New() (string, error)
}

// Random generates version 4 IDs from crypto/rand.
type Random struct{}

// New returns one lowercase RFC 4122 version 4 UUID.
func (Random) New() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", fmt.Errorf("read UUID randomness: %w", err)
	}
	data[6] = (data[6] & 0x0f) | 0x40
	data[8] = (data[8] & 0x3f) | 0x80
	var encoded [36]byte
	hex.Encode(encoded[0:8], data[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], data[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], data[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], data[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], data[10:16])
	return string(encoded[:]), nil
}
