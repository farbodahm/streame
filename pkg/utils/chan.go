package utils

import "sync"

// MergeChannels implements a Fan-In pattern for merging multiple channels into 1
func MergeChannels[k any](channels ...chan (k)) chan (k) {
	out := make(chan k)
	var wg sync.WaitGroup

	// For each input channel, start a goroutine to forward values to the output channel
	for _, ch := range channels {
		wg.Add(1)
		go func(c chan k) {
			defer wg.Done()
			for val := range c {
				out <- val
			}
		}(ch)
	}

	// Start a goroutine to close the output channel when all input channels are done
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
