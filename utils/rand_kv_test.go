package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTestKey(t *testing.T) {
	for i := 0; i < 5; i++ {
		t.Log(string(GetTestKey(i)))
		assert.NotNil(t, string(GetTestKey(i)))
	}
}

func TestRandomValue(t *testing.T) {
	for i := 0; i < 5; i++ {
		assert.NotNil(t, string(RandomValue(10)))
	}
}
