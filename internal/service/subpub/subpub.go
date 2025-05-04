package subpub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var ErrClosed = errors.New("subpub: closed")

type MessageHandler func(msg interface{})

type Subscription interface {
	Unsubscribe()
}

type SubPub interface {
	Subscribe(subject string, cb MessageHandler) (Subscription, error)
	Publish(subject string, msg interface{}) error
	Close(ctx context.Context) error
}

func NewSubPub() SubPub {
	return &subpubImpl{
		subjects: make(map[string]*subjectData),
	}
}

type subpubImpl struct {
	mu           sync.RWMutex
	subjects     map[string]*subjectData
	wg           sync.WaitGroup
	nextSubID    uint64
	closed       atomic.Bool
	shutdownOnce sync.Once
}

type subscriber struct {
	id      uint64
	handler MessageHandler
	msgChan chan interface{}
	quit    chan struct{}
	once    sync.Once
	sp      *subpubImpl
	subject string
}

type subjectData struct {
	mu          sync.RWMutex
	subscribers map[uint64]*subscriber
}

func (s *subscriber) closeChan() {
	s.once.Do(func() {
		close(s.msgChan)
	})
}

func (s *subscriber) cleanupResources() {
	s.closeChan()
}

func (s *subscriber) run(wg *sync.WaitGroup) {
	defer wg.Done()
	defer s.cleanupResources()

	for {
		select {
		case msg, ok := <-s.msgChan:
			if !ok {
				return
			}
			s.handler(msg)
		case <-s.quit:
			return
		}
	}
}

func (sp *subpubImpl) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	if sp.closed.Load() {
		return nil, ErrClosed
	}

	sp.mu.Lock()
	if sp.closed.Load() {
		sp.mu.Unlock()
		return nil, ErrClosed
	}

	sd, ok := sp.subjects[subject]
	if !ok {
		sd = &subjectData{
			subscribers: make(map[uint64]*subscriber),
		}
		sp.subjects[subject] = sd
	}
	sp.mu.Unlock()

	sd.mu.Lock()
	if sp.closed.Load() {
		sd.mu.Unlock()
		return nil, ErrClosed
	}

	subID := atomic.AddUint64(&sp.nextSubID, 1)
	sub := &subscriber{
		id:      subID,
		handler: cb,
		msgChan: make(chan interface{}, 16),
		quit:    make(chan struct{}),
		sp:      sp,
		subject: subject,
	}
	sd.subscribers[subID] = sub
	sd.mu.Unlock()

	sp.wg.Add(1)
	go sub.run(&sp.wg)

	return sub, nil
}

func (s *subscriber) Unsubscribe() {
	s.sp.unsubscribe(s.subject, s.id)
	close(s.quit)
	go func() {
		for range s.msgChan {
		}
	}()
}

func (sp *subpubImpl) Publish(subject string, msg interface{}) error {
	if sp.closed.Load() {
		return ErrClosed
	}

	sp.mu.RLock()
	sd, ok := sp.subjects[subject]
	sp.mu.RUnlock()

	if !ok {
		return nil
	}

	sd.mu.RLock()
	subsToSend := make([]*subscriber, 0, len(sd.subscribers))
	for _, sub := range sd.subscribers {
		subsToSend = append(subsToSend, sub)
	}
	sd.mu.RUnlock()

	for _, sub := range subsToSend {
		select {
		case sub.msgChan <- msg:
		case <-sub.quit:
		default:
			select {
			case sub.msgChan <- msg:
			case <-sub.quit:
			}
		}
	}

	return nil
}

func (sp *subpubImpl) unsubscribe(subject string, subID uint64) {
	sp.mu.RLock()
	sd, ok := sp.subjects[subject]
	sp.mu.RUnlock()

	if !ok {
		return
	}

	sd.mu.Lock()
	delete(sd.subscribers, subID)
	sd.mu.Unlock()
}

func (sp *subpubImpl) Close(ctx context.Context) error {
	var closeErr error
	sp.shutdownOnce.Do(func() {
		sp.mu.Lock()
		if sp.closed.Load() {
			sp.mu.Unlock()
			closeErr = ErrClosed
			return
		}
		sp.closed.Store(true)

		subsToClose := make([]*subscriber, 0)
		for _, sd := range sp.subjects {
			sd.mu.RLock()
			for _, sub := range sd.subscribers {
				subsToClose = append(subsToClose, sub)
			}
			sd.mu.RUnlock()
		}
		sp.subjects = make(map[string]*subjectData)
		sp.mu.Unlock()

		for _, sub := range subsToClose {
			sub.closeChan()
		}

		waitChan := make(chan struct{})
		go func() {
			sp.wg.Wait()
			close(waitChan)
		}()

		select {
		case <-waitChan:
			closeErr = nil
		case <-ctx.Done():
			closeErr = ctx.Err()
		}
	})

	return closeErr
}
