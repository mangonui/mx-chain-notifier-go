package process

import (
	"context"
	"sync"
	"time"

	"github.com/multiversx/mx-chain-core-go/core/check"
	"github.com/multiversx/mx-chain-notifier-go/common"
	"github.com/multiversx/mx-chain-notifier-go/data"
)

const (
	publisherQueueSize   = 1024
	publisherSendTimeout = time.Millisecond
)

type publisher struct {
	handler PublisherHandler

	broadcast                     chan data.BlockEvents
	broadcastRevert               chan data.RevertBlock
	broadcastFinalized            chan data.FinalizedBlock
	broadcastTxs                  chan data.BlockTxs
	broadcastBlockEventsWithOrder chan data.BlockEventsWithOrder
	broadcastScrs                 chan data.BlockScrs
	broadcastStateAccesses        chan data.BlockStateAccesses

	cancelFunc func()
	closeChan  chan struct{}
	mutState   sync.RWMutex
}

// NewPublisher will create a new publisher component
func NewPublisher(handler PublisherHandler) (*publisher, error) {
	if check.IfNil(handler) {
		return nil, ErrNilPublisherHandler
	}

	p := &publisher{
		handler:                       handler,
		broadcast:                     make(chan data.BlockEvents, publisherQueueSize),
		broadcastRevert:               make(chan data.RevertBlock, publisherQueueSize),
		broadcastFinalized:            make(chan data.FinalizedBlock, publisherQueueSize),
		broadcastTxs:                  make(chan data.BlockTxs, publisherQueueSize),
		broadcastScrs:                 make(chan data.BlockScrs, publisherQueueSize),
		broadcastBlockEventsWithOrder: make(chan data.BlockEventsWithOrder, publisherQueueSize),
		broadcastStateAccesses:        make(chan data.BlockStateAccesses, publisherQueueSize),
		closeChan:                     make(chan struct{}),
	}

	return p, nil
}

// Run creates a goroutine and listens for events on the exposed channels
func (p *publisher) Run() error {
	p.mutState.Lock()
	defer p.mutState.Unlock()

	if p.cancelFunc != nil {
		return common.ErrLoopAlreadyStarted
	}

	var ctx context.Context
	ctx, p.cancelFunc = context.WithCancel(context.Background())

	go p.run(ctx)

	return nil
}

func (p *publisher) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			p.handler.Close()
			return
		case events := <-p.broadcast:
			p.handler.Publish(events)
		case revertBlock := <-p.broadcastRevert:
			p.handler.PublishRevert(revertBlock)
		case finalizedBlock := <-p.broadcastFinalized:
			p.handler.PublishFinalized(finalizedBlock)
		case blockTxs := <-p.broadcastTxs:
			p.handler.PublishTxs(blockTxs)
		case blockScrs := <-p.broadcastScrs:
			p.handler.PublishScrs(blockScrs)
		case blockEvents := <-p.broadcastBlockEventsWithOrder:
			p.handler.PublishBlockEventsWithOrder(blockEvents)
		case blockStateAccesses := <-p.broadcastStateAccesses:
			p.handler.PublishStateAccesses(blockStateAccesses)
		}
	}
}

// Broadcast will handle the block events pushed by producers
func (p *publisher) Broadcast(events data.BlockEvents) {
	sendWithTimeout(p.broadcast, events, p.closeChan)
}

// BroadcastRevert will handle the revert event pushed by producers
func (p *publisher) BroadcastRevert(events data.RevertBlock) {
	sendWithTimeout(p.broadcastRevert, events, p.closeChan)
}

// BroadcastFinalized will handle the finalized event pushed by producers
func (p *publisher) BroadcastFinalized(events data.FinalizedBlock) {
	sendWithTimeout(p.broadcastFinalized, events, p.closeChan)
}

// BroadcastTxs will handle the txs event pushed by producers
func (p *publisher) BroadcastTxs(events data.BlockTxs) {
	sendWithTimeout(p.broadcastTxs, events, p.closeChan)
}

// BroadcastScrs will handle the scrs event pushed by producers
func (p *publisher) BroadcastScrs(events data.BlockScrs) {
	sendWithTimeout(p.broadcastScrs, events, p.closeChan)
}

// BroadcastBlockEventsWithOrder will handle the full block events pushed by producers
func (p *publisher) BroadcastBlockEventsWithOrder(events data.BlockEventsWithOrder) {
	sendWithTimeout(p.broadcastBlockEventsWithOrder, events, p.closeChan)
}

// BroadcastStateAccesses will handle state accesses pushed by producers
func (p *publisher) BroadcastStateAccesses(events data.BlockStateAccesses) {
	sendWithTimeout(p.broadcastStateAccesses, events, p.closeChan)
}

func sendWithTimeout[T any](channel chan T, value T, closeChan <-chan struct{}) {
	timer := time.NewTimer(publisherSendTimeout)
	defer timer.Stop()

	select {
	case channel <- value:
	case <-closeChan:
	case <-timer.C:
	}
}

// Close will close the channels
func (p *publisher) Close() error {
	p.mutState.RLock()
	defer p.mutState.RUnlock()

	if p.cancelFunc != nil {
		p.cancelFunc()
	}

	close(p.closeChan)

	return nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (p *publisher) IsInterfaceNil() bool {
	return p == nil
}
