package main

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	etcdEndpoint    = "localhost:2379"
	etcdDialTimeout = 5 * time.Second
	leaseTTL        = 10 // секунды
)

// Coordinator manages shard registration and discovery via etcd.
type Coordinator struct {
	client   *clientv3.Client
	leaseID  clientv3.LeaseID
	shardKey string
}

// NewCoordinator connects to etcd and returns a Coordinator.
func NewCoordinator(instanceID int) (*Coordinator, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: etcdDialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("connect etcd: %w", err)
	}

	return &Coordinator{
		client:   client,
		shardKey: fmt.Sprintf("/mfc/shards/instance-%d", instanceID),
	}, nil
}

// Register registers this instance in etcd with a TTL lease.
// Other instances can discover it via ListInstances.
func (c *Coordinator) Register(ctx context.Context, instanceID int, windows []int) error {
	// Create a lease that expires if not renewed.
	lease, err := c.client.Grant(ctx, leaseTTL)
	if err != nil {
		return fmt.Errorf("grant lease: %w", err)
	}
	c.leaseID = lease.ID

	// Store shard info: which windows this instance owns.
	value := fmt.Sprintf("instance=%d windows=%v", instanceID, windows)
	_, err = c.client.Put(ctx, c.shardKey, value,
		clientv3.WithLease(c.leaseID))
	if err != nil {
		return fmt.Errorf("register shard: %w", err)
	}

	log.Printf("[INFO] coordinator: registered %s = %q (lease=%x)",
		c.shardKey, value, c.leaseID)

	// Start background lease renewal.
	go c.keepAlive(ctx)

	return nil
}

// keepAlive renews the lease periodically until ctx is cancelled.
func (c *Coordinator) keepAlive(ctx context.Context) {
	ch, err := c.client.KeepAlive(ctx, c.leaseID)
	if err != nil {
		log.Printf("[WARN] coordinator: keepalive start failed: %v", err)
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-ch:
			if !ok {
				log.Println("[WARN] coordinator: keepalive channel closed")
				return
			}
			_ = resp
		}
	}
}

// ListInstances returns all registered instances from etcd.
func (c *Coordinator) ListInstances(ctx context.Context) ([]string, error) {
	resp, err := c.client.Get(ctx, "/mfc/shards/",
		clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list instances: %w", err)
	}

	instances := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		instances = append(instances, string(kv.Value))
	}
	return instances, nil
}

// Deregister removes this instance from etcd and revokes the lease.
func (c *Coordinator) Deregister(ctx context.Context) {
	if c.leaseID != 0 {
		_, err := c.client.Revoke(ctx, c.leaseID)
		if err != nil {
			log.Printf("[WARN] coordinator: revoke lease: %v", err)
		}
	}
	c.client.Close()
	log.Println("[INFO] coordinator: deregistered")
}

// AssignShards distributes numWindows windows across instances evenly.
// Returns the slice of window IDs assigned to instanceID.
func AssignShards(instanceID, numInstances, numWindows int) []int {
	var assigned []int
	for i := 1; i <= numWindows; i++ {
		if (i-1)%numInstances == (instanceID - 1) {
			assigned = append(assigned, i)
		}
	}
	return assigned
}