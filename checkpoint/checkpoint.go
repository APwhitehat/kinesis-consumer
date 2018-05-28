package checkpoint

import (
	"errors"
	"fmt"
	"time"
)

// Client defines the DB functionality needed for checkpoint
type Client interface {
	// Get a value in database
	Get(key string) (string, error)
	// Set a value in database
	Set(key string, value string, ttl time.Duration) error
	// Ping the database to check it's working
	Ping() error
}

// New returns a checkpoint that uses Redis for underlying storage
func New(appName string, client Client) (*Checkpoint, error) {
	if client == nil {
		return nil, errors.New("RedisClient reference points to nil")
	}

	// verify we can ping redis server
	err := client.Ping()
	if err != nil {
		return nil, err
	}

	return &Checkpoint{
		appName: appName,
		client:  client,
	}, nil
}

// Checkpoint stores and retreives the last evaluated key from a DB Scan
type Checkpoint struct {
	appName string
	client  Client
}

// Get fetches the checkpoint for a particular Shard.
func (c *Checkpoint) Get(streamName, shardID string) (string, error) {
	val, _ := c.client.Get(c.key(streamName, shardID))
	return val, nil
}

// Set stores a checkpoint for a shard (e.g. sequence number of last record processed by application).
// Upon failover, record processing is resumed from this point.
func (c *Checkpoint) Set(streamName, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		return fmt.Errorf("sequence number should not be empty")
	}
	err := c.client.Set(c.key(streamName, shardID), sequenceNumber, 0)
	if err != nil {
		return err
	}
	return nil
}

// key generates a unique Redis key for storage of Checkpoint.
func (c *Checkpoint) key(streamName, shardID string) string {
	return fmt.Sprintf("%v:checkpoint:%v:%v", c.appName, streamName, shardID)
}
