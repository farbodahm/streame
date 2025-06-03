//go:build integration

package core_test

import (
	"context"
	"testing"
	"time"

	. "github.com/farbodahm/streame/pkg/core"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestLeaderElector_Start_SingleNodeElection verifies that a single node becomes leader
// and invokes the OnStartedLeading callback when participating in leader election.
func TestLeaderElector_Start_SingleNodeElection(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	assert.NoError(t, err)
	defer cli.Close()

	startedCh := make(chan struct{}, 1)
	elector, err := NewLeaderElector("node1", cli,
		func(ctx context.Context) { startedCh <- struct{}{} },
		func() {},
		func(string) {},
	)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Run leader election in background
	go elector.Start(ctx)

	select {
	case <-startedCh:
	// success: OnStartedLeading invoked
	case <-time.After(5 * time.Second):
		t.Fatal("OnStartedLeading callback was not called")
	}
}

// TestLeaderElector_Start_TwoNodeElectionOnNewLeader verifies that when a second node
// participates in the election, it detects the existing leader and executes OnNewLeader callback.
func TestLeaderElector_Start_TwoNodeElectionOnNewLeader(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	assert.NoError(t, err)
	defer cli.Close()

	startedCh := make(chan struct{}, 1)
	newLeaderCh := make(chan string, 1)

	elector1, err := NewLeaderElector("node1", cli,
		func(ctx context.Context) { startedCh <- struct{}{} },
		func() {},
		func(string) {},
	)
	assert.NoError(t, err)

	elector2, err := NewLeaderElector("node2", cli,
		func(ctx context.Context) {},
		func() {},
		func(newLeaderID string) { newLeaderCh <- newLeaderID },
	)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go elector1.Start(ctx)
	select {
	case <-startedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("node1 did not start leading")
	}

	go elector2.Start(ctx)
	select {
	case leaderID := <-newLeaderCh:
		assert.Equal(t, "node1", leaderID)
	case <-time.After(5 * time.Second):
		t.Fatal("node2 did not detect node1 as leader")
	}
}

// TestLeaderElector_Start_OneLeaderOneWorker_OnStoppedLeading verifies that when a leader
// goes down while one worker is participating, OnStoppedLeading callback is called.
func TestLeaderElector_Start_OneLeaderOneWorkerOnStoppedLeading(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	assert.NoError(t, err)
	defer cli.Close()

	leaderStartedCh := make(chan struct{}, 1)
	leaderStoppedCh := make(chan struct{}, 1)
	workerNewLeaderCh := make(chan string, 1)

	elector1, err := NewLeaderElector("node1", cli,
		func(ctx context.Context) { leaderStartedCh <- struct{}{} },
		func() { leaderStoppedCh <- struct{}{} },
		func(string) {},
	)
	assert.NoError(t, err)

	elector2, err := NewLeaderElector("node2", cli,
		func(ctx context.Context) {},
		func() {},
		func(newLeaderID string) { workerNewLeaderCh <- newLeaderID },
	)
	assert.NoError(t, err)

	timeout := 30 * time.Second
	ctx1, cancel1 := context.WithTimeout(context.Background(), timeout)
	defer cancel1()
	ctx2, cancel2 := context.WithTimeout(context.Background(), timeout)
	defer cancel2()

	go elector1.Start(ctx1)
	select {
	case <-leaderStartedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("node1 did not start leading")
	}

	go elector2.Start(ctx2)
	select {
	case <-workerNewLeaderCh:
	case <-time.After(5 * time.Second):
		t.Fatal("node2 did not detect node1 as leader")
	}

	cancel1()

	// Give some time for leader to stop
	time.Sleep(3 * time.Second)

	select {
	case <-leaderStoppedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("OnStoppedLeading callback was not called on node1")
	}
}

// TestLeaderElector_Start_OneLeaderTwoWorkers_OnNewLeaderAfterLeaderStops verifies that when
// a leader goes down with two workers, one worker becomes leader and the other worker's
// OnNewLeader callback is called with the new leader ID.
func TestLeaderElector_Start_OneLeaderTwoWorkersOnNewLeaderAfterLeaderStops(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	assert.NoError(t, err)
	defer cli.Close()

	leaderStartedCh := make(chan struct{}, 1)
	worker2StartedCh := make(chan string, 1)
	worker2NewLeaderCh := make(chan string, 1)
	worker3StartedCh := make(chan string, 1)
	worker3NewLeaderCh := make(chan string, 1)

	elector1, err := NewLeaderElector("node1", cli,
		func(ctx context.Context) { leaderStartedCh <- struct{}{} },
		func() {},
		func(string) {},
	)
	assert.NoError(t, err)

	elector2, err := NewLeaderElector("node2", cli,
		func(ctx context.Context) { worker2StartedCh <- "node2" },
		func() {},
		func(newLeaderID string) { worker2NewLeaderCh <- newLeaderID },
	)
	assert.NoError(t, err)

	elector3, err := NewLeaderElector("node3", cli,
		func(ctx context.Context) { worker3StartedCh <- "node3" },
		func() {},
		func(newLeaderID string) { worker3NewLeaderCh <- newLeaderID },
	)
	assert.NoError(t, err)

	timeout := 30 * time.Second
	ctx1, cancel1 := context.WithTimeout(context.Background(), timeout)
	defer cancel1()
	ctx2, cancel2 := context.WithTimeout(context.Background(), timeout)
	defer cancel2()
	ctx3, cancel3 := context.WithTimeout(context.Background(), timeout)
	defer cancel3()

	go elector1.Start(ctx1)
	select {
	case <-leaderStartedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("node1 did not start leading")
	}

	go elector2.Start(ctx2)
	go elector3.Start(ctx3)

	select {
	case id := <-worker2NewLeaderCh:
		assert.Equal(t, "node1", id)
	case <-time.After(5 * time.Second):
		t.Fatal("node2 did not detect node1 as leader")
	}
	select {
	case id := <-worker3NewLeaderCh:
		assert.Equal(t, "node1", id)
	case <-time.After(5 * time.Second):
		t.Fatal("node3 did not detect node1 as leader")
	}

	cancel1()

	var newLeader string
	select {
	case newLeader = <-worker2StartedCh:
	case newLeader = <-worker3StartedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("no new leader was elected")
	}

	if newLeader == "node2" {
		select {
		case id := <-worker3NewLeaderCh:
			assert.Equal(t, "node2", id)
		case <-time.After(5 * time.Second):
			t.Fatal("node3 did not detect node2 as new leader")
		}
	} else if newLeader == "node3" {
		select {
		case id := <-worker2NewLeaderCh:
			assert.Equal(t, "node3", id)
		case <-time.After(5 * time.Second):
			t.Fatal("node2 did not detect node3 as new leader")
		}
	} else {
		t.Fatalf("unexpected new leader: %s", newLeader)
	}
}
