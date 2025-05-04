package subpub

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewSubPub(t *testing.T) {
	sp := NewSubPub()
	if sp == nil {
		t.Fatal("newsub returned nil")
	}
	spImpl, ok := sp.(*subpubImpl)
	if !ok {
		t.Fatal("newsub did not return expected type")
	}
	if len(spImpl.subjects) != 0 {
		t.Errorf("expected empty subjects got size %d", len(spImpl.subjects))
	}
	if spImpl.closed.Load() {
		t.Error("expected initial closed state false")
	}
}

func TestBasicPubSub(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()
	defer sp.Close(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	var receivedMsg interface{}
	var received bool

	_, err := sp.Subscribe("topic1", func(msg interface{}) {
		receivedMsg = msg
		received = true
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	testPayload := "hello world"
	err = sp.Publish("topic1", testPayload)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	waitTimeout(&wg, 1*time.Second, t)

	if !received {
		t.Error("MessageHandler was not called")
	}
	if receivedMsg != testPayload {
		t.Errorf("expected message %v got %v", testPayload, receivedMsg)
	}
}

func TestMultipleSubscribersSameTopic(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()
	defer sp.Close(context.Background())

	var wg sync.WaitGroup
	count := 3
	wg.Add(count)
	var receivedCount int32

	for i := 0; i < count; i++ {
		_, err := sp.Subscribe("topic1", func(msg interface{}) {
			if msgStr, ok := msg.(string); !ok || msgStr != "data" {
				t.Errorf("Subscriber received unexpected message: %v", msg)
			}
			atomic.AddInt32(&receivedCount, 1)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}
	}

	err := sp.Publish("topic1", "data")
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	waitTimeout(&wg, 1*time.Second, t)

	if atomic.LoadInt32(&receivedCount) != int32(count) {
		t.Errorf("expected %d subscribers to receive message but got %d", count, atomic.LoadInt32(&receivedCount))
	}
}

func TestMultipleTopics(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()
	defer sp.Close(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)

	var topic1Received atomic.Bool
	var topic2Received atomic.Bool

	_, err := sp.Subscribe("topic1", func(msg interface{}) {
		topic1Received.Store(true)
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Subscribe topic1 failed: %v", err)
	}

	_, err = sp.Subscribe("topic2", func(msg interface{}) {
		topic2Received.Store(true)

		t.Error("Subscriber for topic2 received message intended for topic1")
	})
	if err != nil {
		t.Fatalf("Subscribe topic2 failed: %v", err)
	}

	err = sp.Publish("topic1", "message for topic1")
	if err != nil {
		t.Fatalf("Publish to topic1 failed: %v", err)
	}

	waitTimeout(&wg, 1*time.Second, t)

	if !topic1Received.Load() {
		t.Error("Subscriber for topic1 didnt receive message")
	}
	if topic2Received.Load() {
		t.Error("Subscriber for topic2 received wrong message for topic1")
	}
}

func TestUnsubscribe(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()
	defer sp.Close(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	var receivedCount int32

	sub, err := sp.Subscribe("topic1", func(msg interface{}) {
		atomic.AddInt32(&receivedCount, 1)
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	err = sp.Publish("topic1", "first")
	if err != nil {
		t.Fatalf("Publish first failed: %v", err)
	}
	waitTimeout(&wg, 1*time.Second, t)

	sub.Unsubscribe()

	time.Sleep(50 * time.Millisecond)

	err = sp.Publish("topic1", "second")
	if err != nil {
		t.Fatalf("Publish second failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if count := atomic.LoadInt32(&receivedCount); count != 1 {
		t.Errorf("Expected only 1 message to be received, got %d", count)
	}
}

func TestFIFOOrder(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()
	defer sp.Close(context.Background())

	var wg sync.WaitGroup
	numMessages := 100
	wg.Add(numMessages)
	receivedMessages := make([]int, 0, numMessages)
	var mu sync.Mutex

	_, err := sp.Subscribe("fifo_topic", func(msg interface{}) {
		if val, ok := msg.(int); ok {
			mu.Lock()
			receivedMessages = append(receivedMessages, val)
			mu.Unlock()
		} else {
			t.Errorf("received non int message: %v", msg)
		}
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	for i := 0; i < numMessages; i++ {
		err = sp.Publish("fifo_topic", i)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	waitTimeout(&wg, 5*time.Second, t)

	mu.Lock()
	defer mu.Unlock()
	if len(receivedMessages) != numMessages {
		t.Fatalf("expected %d messages, received %d", numMessages, len(receivedMessages))
	}
	for i := 0; i < numMessages; i++ {
		if receivedMessages[i] != i {
			t.Errorf("fifo ruined, expected %d at %d got %d", i, i, receivedMessages[i])
			t.Logf("Received: %v", receivedMessages)
			break
		}
	}
}

func TestSlowSubscriber(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()
	defer sp.Close(context.Background())

	var wg sync.WaitGroup
	wg.Add(2)

	var fastReceived, slowReceived atomic.Bool
	var fastReceivedTime, slowReceivedTime time.Time
	var slowHandlerStart time.Time

	_, err := sp.Subscribe("slow_topic", func(msg interface{}) {
		fastReceivedTime = time.Now()
		fastReceived.Store(true)
		wg.Done()
	})
	if err != nil {
		t.Fatalf("fast subscribe failed: %v", err)
	}

	_, err = sp.Subscribe("slow_topic", func(msg interface{}) {
		slowHandlerStart = time.Now()
		time.Sleep(200 * time.Millisecond)
		slowReceivedTime = time.Now()
		slowReceived.Store(true)
		wg.Done()
	})
	if err != nil {
		t.Fatalf("slow subscribe failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	publishTime := time.Now()
	err = sp.Publish("slow_topic", "message")
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	waitTimeout(&wg, 5*time.Second, t)

	if !fastReceived.Load() {
		t.Error("fast subscriber didnt receive message")
	}
	if !slowReceived.Load() {
		t.Error("Slow subscriber didnt receive message")
	}

	if fastReceivedTime.Sub(publishTime) > 100*time.Millisecond {
		t.Errorf("fast took too long: %v", fastReceivedTime.Sub(publishTime))
	}
	if slowHandlerStart.Sub(publishTime) > 100*time.Millisecond {
		t.Errorf("slow handler started too late: %v", slowHandlerStart.Sub(publishTime))
	}
	if slowReceivedTime.Sub(slowHandlerStart) < 190*time.Millisecond {
		t.Errorf("slow finished too quickly, sleep: %v", slowReceivedTime.Sub(slowHandlerStart))
	}
	if fastReceivedTime.After(slowHandlerStart.Add(50 * time.Millisecond)) {
		t.Errorf("fast finished (%v) too late after slow handler started (%v)", fastReceivedTime.Sub(publishTime), slowHandlerStart.Sub(publishTime))
	}

}

func TestCloseContextCancelled(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()

	handlerStarted := make(chan struct{})
	handlerFinished := make(chan struct{})
	_, err := sp.Subscribe("topic_close", func(msg interface{}) {
		close(handlerStarted)
		time.Sleep(5 * time.Second)
		close(handlerFinished)
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	err = sp.Publish("topic_close", "test")
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	<-handlerStarted

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	closeStartTime := time.Now()
	closeErr := sp.Close(ctx)
	closeDuration := time.Since(closeStartTime)

	if closeErr == nil {
		t.Error("expected context cancelled error got nil")
	}
	if !errors.Is(closeErr, context.DeadlineExceeded) && !errors.Is(closeErr, context.Canceled) {
		t.Errorf("expected context error got %v", closeErr)
	}

	if closeDuration > 150*time.Millisecond {
		t.Errorf("close took too long (%v)", closeDuration)
	}

	select {
	case <-handlerFinished:
		t.Error("handler finished execution but close context was cancelled before")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestCloseGracefulShutdown(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()

	handlerDelay := 100 * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(1)

	handlerFinishedTime := time.Time{}
	var handlerFinished atomic.Bool

	_, err := sp.Subscribe("topic_graceful", func(msg interface{}) {
		time.Sleep(handlerDelay)
		handlerFinishedTime = time.Now()
		handlerFinished.Store(true)
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	err = sp.Publish("topic_graceful", "test")
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	closeStartTime := time.Now()
	closeErr := sp.Close(ctx)
	closeEndTime := time.Now()
	closeDuration := closeEndTime.Sub(closeStartTime)

	if closeErr != nil {
		t.Errorf("expected close, got %v", closeErr)
	}

	waitTimeout(&wg, 1*time.Second, t)

	if !handlerFinished.Load() {
		t.Fatal("handler didnt finish executing")
	}

	if closeDuration < handlerDelay-(25*time.Millisecond) {
		t.Errorf("close returned too quickly (%v)", closeDuration)
	}

	if handlerFinishedTime.After(closeEndTime) {
		t.Errorf("handler finished (%v) after close (%v)", handlerFinishedTime, closeEndTime)
	}
}

func TestPublishAfterClose(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()

	_, err := sp.Subscribe("topic", func(msg interface{}) {
		t.Error("handler called after close")
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	err = sp.Close(context.Background())
	if err != nil {
		t.Fatalf("Close failed unexpectedly: %v", err)
	}

	err = sp.Publish("topic", "message")
	if err == nil {
		t.Error("publish after close succeeded, expected error")
	}
	if !errors.Is(err, ErrClosed) {
		t.Errorf("expected ErrClosed from publish, got %v", err)
	}
}

func TestSubscribeAfterClose(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()

	err := sp.Close(context.Background())
	if err != nil {
		t.Fatalf("close failed unexpectedly: %v", err)
	}

	_, err = sp.Subscribe("topic", func(msg interface{}) {
		t.Error("hanler created after close")
	})
	if err == nil {
		t.Error("subscribe after close succeeded, expected error")
	}
	if !errors.Is(err, ErrClosed) {
		t.Errorf("expected ErrClosed from subscribe, got %v", err)
	}
}

func TestConcurrentPubSubUnsub(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()
	defer sp.Close(context.Background())

	numGoroutines := 50
	numMessagesPerG := 20
	topic := "concurrent_topic"

	var opsCount atomic.Uint64
	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3)

	for i := 0; i < numGoroutines; i++ {
		go func(gID int) {
			defer wg.Done()
			for j := 0; j < numMessagesPerG; j++ {
				msg := fmt.Sprintf("g%d-msg%d", gID, j)
				if err := sp.Publish(topic, msg); err != nil && !errors.Is(err, ErrClosed) {
				}
				opsCount.Add(1)
				runtime.Gosched()
			}
		}(i)
	}

	var subscriptions sync.Map
	var subCounter atomic.Uint64

	for i := 0; i < numGoroutines; i++ {
		go func(gID int) {
			defer wg.Done()
			var receivedInG atomic.Uint64
			sub, err := sp.Subscribe(topic, func(msg interface{}) {
				receivedInG.Add(1)
				opsCount.Add(1)
			})
			if err == nil {
				subID := subCounter.Add(1)
				subscriptions.Store(subID, sub)
			} else if !errors.Is(err, ErrClosed) {
			}
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(50+i%10) * time.Millisecond)
			for j := 0; j < 5; j++ {
				var subToUnsub Subscription
				var keyToDel any
				found := false
				subscriptions.Range(func(key, value interface{}) bool {
					subToUnsub = value.(Subscription)
					keyToDel = key
					found = true
					return false
				})

				if found {
					if _, loaded := subscriptions.LoadAndDelete(keyToDel); loaded {
						subToUnsub.Unsubscribe()
						opsCount.Add(1)
					}
				}
				time.Sleep(time.Duration(10+i%5) * time.Millisecond)
			}
		}()
	}

	wg.Wait()

	t.Logf("total operations: %d", opsCount.Load())
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration, t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("WaitGroup wait timed out %v", timeout)
	}
}
