package channelmanager

import (
	"context"
	"fmt"
	"sync"
)

// ChannelManager manages producers writing to a channel and handles closing the channel safely.
type ChannelManager[T any] struct {
	dataCh chan T
	errCh  chan error
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewChannelManager creates a new ChannelManager for the given channel buffer size.
func NewChannelManager[T any](bufferSize int) *ChannelManager[T] {
	ctx, cancel := context.WithCancel(context.Background())
	return &ChannelManager[T]{
		dataCh: make(chan T, bufferSize),
		errCh:  make(chan error, 1), // Ensure error channel is non-blocking
		ctx:    ctx,
		cancel: cancel,
	}
}

// Subscribe adds a producer to the ChannelManager and runs the producer in a goroutine.
// If the producer returns an error, it sends the error to the error channel and cancels the context.
func (cm *ChannelManager[T]) Subscribe(producer func(chan<- T) error) {
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		if err := producer(cm.dataCh); err != nil {
			cm.errCh <- err
			cm.cancel()
		}
	}()
}

// Process starts processing data and error channels with the given callback.
// The callback processes each data item and can handle context cancellation.
func (cm *ChannelManager[T]) Process(callback func(data T) error) error {
	for {
		select {
		case <-cm.ctx.Done():
			return nil
		case err, ok := <-cm.errCh:
			if !ok {
				return nil
			}
			return err
		default:
			for data := range cm.dataCh {
				if err := callback(data); err != nil {
					return fmt.Errorf("callback error: %w", err)
				}
			}
		}
	}
}

// Close ensures all producers complete and then closes the data and error channels.
func (cm *ChannelManager[T]) Close() {
	cm.wg.Wait()
	close(cm.dataCh)
	close(cm.errCh)
}

// DataChannel returns the underlying data channel.
func (cm *ChannelManager[T]) DataChannel() <-chan T {
	return cm.dataCh
}

// ErrorChannel returns the underlying error channel.
func (cm *ChannelManager[T]) ErrorChannel() <-chan error {
	return cm.errCh
}
