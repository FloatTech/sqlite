package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTags(t *testing.T) {
	type test struct {
		a string
		B bool
		c int
		D []byte
	}
	assert.Equal(t, []string{"a", "B", "c", "D"}, tags(&test{}))
}
