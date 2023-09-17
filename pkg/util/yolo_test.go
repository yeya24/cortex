package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestYoloBuf(t *testing.T) {
	s := YoloBuf("hello world")

	require.Equal(t, []byte("hello world"), s)
}

func TestYoloString(t *testing.T) {
	s := YoloString([]byte("hello world"))

	require.Equal(t, "hello world", s)
}
