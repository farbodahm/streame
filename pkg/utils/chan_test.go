package utils_test

import (
	"testing"

	"github.com/farbodahm/streame/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestMergeChannels_SimpleMerge_ShouldMergeTwoChannels(t *testing.T) {
	ch1 := make(chan int)
	ch2 := make(chan int)
	out := utils.MergeChannels(ch1, ch2)

	go func() {
		defer close(ch1)
		ch1 <- 1
		ch1 <- 2
	}()

	go func() {
		defer close(ch2)
		ch2 <- 3
		ch2 <- 4
	}()

	expected := map[int]bool{1: true, 2: true, 3: true, 4: true}
	for val := range out {
		assert.True(t, expected[val], "Unexpected value received: %d", val)
		delete(expected, val)
	}

	assert.Empty(t, expected, "Not all expected values were received. Missing: %v", expected)
}

func TestMergeChannels_EmptyChannels_ShouldCloseWithoutData(t *testing.T) {
	ch1 := make(chan int)
	ch2 := make(chan int)
	close(ch1)
	close(ch2)

	out := utils.MergeChannels(ch1, ch2)

	zero_value, ok := <-out

	assert.False(t, ok, "Expected channel to be closed")
	assert.Equal(t, 0, zero_value, "Expected zero value for a closed channel")
}

func TestMergeChannels_ChannelsCloseAtDifferentTimes_ShouldHandleGracefully(t *testing.T) {
	ch1 := make(chan int)
	ch2 := make(chan int)
	out := utils.MergeChannels(ch1, ch2)

	go func() {
		defer close(ch1)
		ch1 <- 1
	}()

	go func() {
		defer close(ch2)
		ch2 <- 2
		ch2 <- 3
	}()

	expected := map[int]bool{1: true, 2: true, 3: true}
	for val := range out {
		assert.True(t, expected[val], "Unexpected value received: %d", val)
		delete(expected, val)
	}

	assert.Empty(t, expected, "Not all expected values were received. Missing: %v", expected)
	_, ok := <-out
	assert.False(t, ok, "Expected channel to be closed")
}
