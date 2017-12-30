package kinesis

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

type TestLogger struct {
	t *testing.T
}

func (l *TestLogger) Logf(msg string, args ...interface{}) {
	l.t.Logf(msg, args...)
}
func (l *TestLogger) Errorf(msg string, args ...interface{}) {
	l.t.Logf("ERROR: "+msg, args...)
}
func TestAPISyncerInsertsShards(t *testing.T) {
	DefaultLogger = &TestLogger{t}
	//start 1 worker with 12 shards
	c := NewMockConsumer(t)
	numShards := 12
	k := NewKinesisProxyStub().WithShards(numShards)

	config := &Config{
		StreamName:                "testing",
		InitialPositionInStream:   NewInitialPositionInStreamLatest(),
		ShardSyncFromAPIFrequency: time.Second,
	}

	repo := NewLeaseRepoMock()
	w, err := NewWorker(c, k, repo, config)
	if err != nil {
		t.Fatalf("unexpected err starting worker: %v", err)
	}

	go func() {
		err = w.Start()
		if err != nil {
			t.Errorf("unexpected err: %v", err)
		}
	}()

	time.Sleep(time.Millisecond * 1500)
	w.Stop()

	//should have leases equal to number of shards
	if len(repo.createLeaseCalls) != numShards {
		t.Errorf("expected %d leases to be inserted to repo, got %d", numShards, len(repo.createLeaseCalls))
	}
}

func TestLeasesAreCreatedInRepoForEachShard(t *testing.T) {
	DefaultLogger = &TestLogger{t}
	//start 1 worker with 12 shards
	c := NewMockConsumer(t)
	numShards := 12
	k := NewKinesisProxyStub().WithShards(numShards)

	config := &Config{
		StreamName:                "testing",
		InitialPositionInStream:   NewInitialPositionInStreamLatest(),
		ShardSyncFromAPIFrequency: time.Second,
	}

	repo := NewLeaseRepoMock()
	w, err := NewWorker(c, k, repo, config)
	if err != nil {
		t.Fatalf("unexpected err starting worker: %v", err)
	}

	go func() {
		err = w.Start()
		if err != nil {
			t.Errorf("unexpected err: %v", err)
		}
	}()

	time.Sleep(time.Millisecond * 1500)
	w.Stop()

	//should have leases equal to number of shards
	if len(repo.createLeaseCalls) != numShards {
		t.Errorf("expected %d leases to be inserted to repo, got %d", numShards, len(repo.createLeaseCalls))
	}
}

func TestConsumerGetRecords(t *testing.T) {
	DefaultLogger = &TestLogger{t}
	c := NewMockConsumer(t)
	numShards := 2
	k := NewKinesisProxyStub().
		WithShards(numShards)
	records := map[string][]*kinesis.Record{
		"1": []*kinesis.Record{
			{Data: []byte("1")},
			{Data: []byte("2")},
			{Data: []byte("3")},
		},
	}
	for shard, records := range records {
		k.WithShardRecords(shard, records)
	}

	config := &Config{
		StreamName:                "testing",
		InitialPositionInStream:   NewInitialPositionInStreamLatest(),
		ShardSyncFromAPIFrequency: time.Second,
		TakeAndReleaseLeasesFreq:  time.Second,
		LeaseSyncFromRepoFreq:     time.Second,
	}

	repo := NewLeaseRepoMock()
	w, err := NewWorker(c, k, repo, config)
	if err != nil {
		t.Fatalf("unexpected err starting worker: %v", err)
	}

	go func() {
		err = w.Start()
		if err != nil {
			t.Errorf("unexpected err: %v", err)
		}
	}()

	time.Sleep(time.Second * 5)
	w.Stop()

	//should have received the records
	for shard, expectedRecords := range records {
		got, ok := c.receivedRecords[shard]
		if !ok {
			t.Errorf("no records set for shard %s", shard)
			continue
		}
		if !reflect.DeepEqual(expectedRecords, got) {
			t.Errorf("expected records for shard %s:\n%v\ngot:\n%v", shard, expectedRecords, got)
		}
	}
}

func NewMockConsumer(t *testing.T) *MockConsumer {
	return &MockConsumer{
		Logger:          &TestLogger{t},
		receivedRecords: map[string][]*kinesis.Record{},
	}
}

type MockConsumer struct {
	Logger          Logger
	receivedRecords map[string][]*kinesis.Record
	m               sync.Mutex
}

func (c *MockConsumer) ProcessRecords(shardID string, records []*kinesis.Record) error {
	c.m.Lock()
	defer c.m.Unlock()
	c.receivedRecords[shardID] = records
	return nil
}
func (c *MockConsumer) GetReceivedRecords(shardID string) []*kinesis.Record {
	c.m.Lock()
	defer c.m.Unlock()
	return c.receivedRecords[shardID]
}
func (c *MockConsumer) Shutdown(*Checkpointer) error {
	return nil
}
