package core

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	etcdv3 "go.etcd.io/etcd/client/v3"
	concurrency "go.etcd.io/etcd/client/v3/concurrency"
)

const (
	// LeaderElectionKey is the key used for leader election in etcd.
	LeaderElectionKey = "/streame/leader-election"
)

type LeaderElector struct {
	etcd *etcdv3.Client

	// NodeID is the unique identifier for this node in the cluster.
	NodeID string
	// OnStartedLeading is a callback function that is called when this node becomes the leader.
	OnStartedLeading func(context.Context)
	// OnStoppedLeading is a callback function that is called when this node stops being the leader.
	OnStoppedLeading func()
	// OnNewLeader is a callback function that is called when a new leader is elected.
	// It receives the ID of the new leader.
	OnNewLeader func(newLeaderID string)

	currentLeaderID string
}

func NewLeaderElector(nodeID string,
	etcdClient *etcdv3.Client,
	onStartedLeading func(context.Context),
	onStoppedLeading func(),
	onNewLeader func(newLeaderID string)) (*LeaderElector, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("nodeID cannot be empty")
	}
	if onStartedLeading == nil {
		return nil, fmt.Errorf("onStartedLeading callback cannot be nil")
	}
	if onStoppedLeading == nil {
		return nil, fmt.Errorf("onStoppedLeading callback cannot be nil")
	}
	if onNewLeader == nil {
		return nil, fmt.Errorf("onNewLeader callback cannot be nil")
	}

	return &LeaderElector{
		NodeID:           nodeID,
		OnStartedLeading: onStartedLeading,
		OnStoppedLeading: onStoppedLeading,
		OnNewLeader:      onNewLeader,
		etcd:             etcdClient,
	}, nil
}

// Start starts the leader election process.
func (le *LeaderElector) Start(ctx context.Context) error {
	session, err := concurrency.NewSession(le.etcd, concurrency.WithTTL(2))
	if err != nil {
		slog.Error("Failed to create etcd session", "error", err)
		return err
	}
	defer func() {
		slog.Info("Closing etcd session", "nodeID", le.NodeID)
		err := session.Close()
		if err != nil {
			slog.Error("Failed to close etcd session", "error", err)
		}
		if le.IsLeader() {
			slog.Info("Stopped leading due to execution finished", "nodeID", le.NodeID)
			le.OnStoppedLeading()
		}
	}()

	election := concurrency.NewElection(session, LeaderElectionKey)

	go func() {
		err := le.tryBeLeader(ctx, election)
		if err != nil {
			slog.Error("Failed to become leader", "error", err)
			return
		}
		le.OnStartedLeading(ctx)
		slog.Info("Started leading", "nodeID", le.NodeID)
	}()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Leader election stopped", "nodeID", le.NodeID)
			return ctx.Err()

		default:
			leaderResp, err := election.Leader(ctx)
			if err != nil {
				// Possibly no leader yet; log and retry
				slog.Warn("No leader yet or leader query failed", "error", err)
				time.Sleep(1 * time.Second)
				continue
			}

			leaderID := string(leaderResp.Kvs[0].Value)
			if leaderID != le.currentLeaderID {
				if leaderID != le.NodeID {
					slog.Info("Leader changed", "oldLeaderID", le.currentLeaderID, "newLeaderID", leaderID)
					le.OnNewLeader(leaderID)
				}

				// If the old leader was this node and it's no longer the leader
				if le.IsLeader() && leaderID != le.NodeID {
					slog.Info("Stopped leading due to leader changed", "nodeID", le.NodeID)
					le.OnStoppedLeading()
				}

				le.currentLeaderID = leaderID
			}

			time.Sleep(2 * time.Second) // Poll interval
		}
	}
}

// isLeader checks if this node is the current leader.
func (le *LeaderElector) IsLeader() bool {
	return le.currentLeaderID == le.NodeID
}

// tryBeLeader attempts to become the leader by creating an election.
// This is a blocking call that will return when the node becomes the leader or an error occurs.
func (le *LeaderElector) tryBeLeader(ctx context.Context, election *concurrency.Election) error {
	slog.Info("Attempting to become leader", "nodeID", le.NodeID)

	if err := election.Campaign(ctx, le.NodeID); err != nil {
		slog.Error("Failed to campaign for leadership", "error", err)
		return err
	}
	slog.Info("Became leader", "nodeID", le.NodeID)
	le.currentLeaderID = le.NodeID
	return nil
}
