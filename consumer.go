package kinesis

import (
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/satori/go.uuid"
)

const (
	errConfigStreamNameRequired                            = Error("config: stream name required")
	errConfigInitialPositionInStreamRequired               = Error("config: intial position in stream required")
	errConfigLeaseTTLMustBeGreaterThanZero                 = Error("config: lease ttl must be greater than zero")
	errConfigLeaseRenewFreqMustBeGreaterThanZero           = Error("config: lease renew frequency must be greater than zero")
	errConfigShardSyncFreqMustBeGreaterThanZero            = Error("config: shard sync frequency must be greater than zero")
	errConfigTakeAndReleaseLeasesFreqMustBeGreaterThanZero = Error("config: take and release lease frequency must be greater than zero")
)

type Worker struct {
	logger       Logger
	config       *Config
	consumer     Consumer
	leaseManager *leaseManager
	kinesis      kinesisiface.KinesisAPI

	runningShards map[string]func(ShutdownType) //shard id to shard consumer shutdown channel
	consumerExit  chan shardConsumerExit
	shutdown      chan struct{}
}

//TODO: refactor to make worker creation more user friendly (pass in AWS clients/session)
func NewWorker(consumer Consumer, k kinesisiface.KinesisAPI, l leaseRepo, c *Config) (*Worker, error) {
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	w := &Worker{
		logger:        DefaultLogger,
		config:        c,
		kinesis:       k,
		consumer:      consumer,
		shutdown:      make(chan struct{}),
		runningShards: map[string]func(ShutdownType){},
	}
	//create table if not exists
	if err := l.InitRepository(); err != nil {
		return nil, err
	}

	w.logger.Logf("Worker ID: %s", w.config.WorkerID)

	w.leaseManager = newLeaseManager(l, k, w.config)
	return w, nil
}

//blocks unless an error occurs
//1. starts lease manager
//2. lease manager tells worker which shards to process (shard and checkpoint position)
//3. shard processor keeps fetching/processing/checkpointing unless
//	1. the lease manager sends a notification that the lease was lost
//	2. upon checkpointing, an error is returned indicating the lease was lost
func (w *Worker) Start() error { //TODO: is this error necessary?
	newLeases, lostLeases := w.leaseManager.Start()
	w.consumerExit = make(chan shardConsumerExit)
	for {
		select {
		case l := <-newLeases:
			w.runningShards[l.ShardID] = w.startNewShardConsumer(l)
		case l := <-lostLeases:
			if shutdown, ok := w.runningShards[l.ShardID]; ok {
				shutdown(ShutdownTypeZombie)
			} else {
				w.logger.Errorf("received lease lost signal but shard consumer doesn't exist: %v", l)
			}
		case l := <-w.consumerExit:
			//lost lease, determined during UpdateCheckpoint call
			if _, ok := l.err.(LostLeaseError); ok {
				//the goroutine has already shutdown, just remove from map
				delete(w.runningShards, l.lease.ShardID)
				continue
			}

			if l.err != nil {
				//an error occurred with an external call, restart consumer
				w.runningShards[l.lease.ShardID] = w.startNewShardConsumer(l.lease)
				continue
			}
		case <-w.shutdown:
			for _, shutdown := range w.runningShards {
				shutdown(ShutdownTypeGraceful)
			}
			return nil
		}
	}
}

//TODO: clean this up, check goroutines exited, kill if timeout, etc
func (w *Worker) Stop() {
	close(w.shutdown)
}

type shardConsumerExit struct {
	lease ShardLease
	err   error
}

type ShutdownType int

const (
	ShutdownTypeZombie ShutdownType = iota
	ShutdownTypeGraceful
)

//returns a function to shutdown the consumer, a ShutdownType should be passed in to indicate
//whether the goroutine should shutdown gracefully (try to finish the current batch) or exit
//immediately
func (w *Worker) startNewShardConsumer(l ShardLease) (shutdown func(ShutdownType)) {
	w.logger.Logf("starting shard consumer for shard %s", l.ShardID)
	sd := make(chan ShutdownType)
	shutdown = func(s ShutdownType) {
		sd <- s
	}

	go func() {
		exit := shardConsumerExit{lease: l}
		defer func() {
			//notify that consumer exited
			w.consumerExit <- exit
		}()
		out, err := w.kinesis.GetShardIterator(l.getShardIteratorInput())
		if err != nil {
			w.logger.Errorf("worker: error getting shard iterator: %v", err)
			exit.err = err
			return
		}
		shardIterator := out.ShardIterator
		for {
			select {
			case s := <-sd:
				switch s {
				case ShutdownTypeZombie:
					//a zombie shutdown means a lease was stolen, just exit
					return
				case ShutdownTypeGraceful:
					//a graceful shutdown could interupt a ProcessRecords call but with the current
					//syncronization, it can't, so we still just return. in the future, this could be
					//made more robust
					return
				}
			default:
				//default action is to keep processing records
				//TODO: need to backoff in the case of throttling/errors
				recOut, err := w.kinesis.GetRecords(&kinesis.GetRecordsInput{
					Limit:         &w.config.MaxRecordsPerBatch,
					ShardIterator: shardIterator,
				})
				if err != nil {
					w.logger.Errorf("worker: error getting records: %v", err)
					exit.err = err
					return
				}
				if len(recOut.Records) > 0 {
					err = w.consumer.ProcessRecords(l.ShardID, recOut.Records)
					if err != nil {
						w.logger.Errorf("worker: err processing batch: %v", err)
						//continue to retry the batch
						continue
					}
					//batch was successfully processed
				}
				if len(recOut.Records) == 0 && w.config.PauseBetweenEmptyGetRecordRequests > 0 {
					w.logger.Logf("worker: no records for shard %s, sleeping for %s", l.ShardID, w.config.PauseBetweenEmptyGetRecordRequests)
					time.Sleep(w.config.PauseBetweenEmptyGetRecordRequests)
				}
				if recOut.NextShardIterator == nil { //shard is closed, checkpoint and exit
					//TODO: need counter
					if err := w.leaseManager.UpdateCheckpoint(l.ShardID, CheckpointShardClosed, w.config.WorkerID, 0); err != nil {
						exit.err = err
						return
					}
					w.logger.Logf("worker: shard closed, shard=%s", l.ShardID)
					return
				}
				//checkpoint and process next batch
				//TODO: need counter
				if err := w.leaseManager.UpdateCheckpoint(l.ShardID, *recOut.NextShardIterator, w.config.WorkerID, 0); err != nil {
					exit.err = err
					return
				}
				shardIterator = recOut.NextShardIterator
			}
		}
	}()

	return
}

type Config struct {
	WorkerID           string
	StreamName         string
	MaxRecordsPerBatch int64

	DynamoDBLeaseTableName       string
	DynamoDBInitialReadCapacity  int64
	DynamoDBInitialWriteCapacity int64

	LeaseTTL                           time.Duration
	LeaseRenewFrequency                time.Duration
	ShardSyncFromAPIFrequency          time.Duration
	LeaseSyncFromRepoFreq              time.Duration
	TakeAndReleaseLeasesFreq           time.Duration
	InitialPositionInStream            InitialPositionInStream
	PauseBetweenEmptyGetRecordRequests time.Duration
}

func (c *Config) applyDefaults() {
	if c.WorkerID == "" {
		c.WorkerID = uuid.NewV4().String()
	}
	if c.MaxRecordsPerBatch == 0 {
		c.MaxRecordsPerBatch = 10000
	}
	if c.LeaseTTL == 0 {
		c.LeaseTTL = 10 * time.Second
	}
	if c.ShardSyncFromAPIFrequency == 0 {
		c.ShardSyncFromAPIFrequency = time.Minute
	}
	if c.LeaseRenewFrequency == 0 {
		c.LeaseRenewFrequency = 5 * time.Second
	}
	if c.LeaseSyncFromRepoFreq == 0 {
		c.LeaseSyncFromRepoFreq = 5 * time.Second
	}
	if c.TakeAndReleaseLeasesFreq == 0 {
		c.TakeAndReleaseLeasesFreq = c.LeaseSyncFromRepoFreq
	}
}

func (c *Config) Validate() error {
	if c.StreamName == "" {
		return errConfigStreamNameRequired
	}
	if c.InitialPositionInStream.position == "" {
		return errConfigInitialPositionInStreamRequired
	}
	if c.LeaseTTL == 0 {
		return errConfigLeaseTTLMustBeGreaterThanZero
	}
	if c.LeaseRenewFrequency == 0 {
		return errConfigLeaseRenewFreqMustBeGreaterThanZero
	}
	if c.ShardSyncFromAPIFrequency == 0 {
		return errConfigShardSyncFreqMustBeGreaterThanZero
	}
	if c.TakeAndReleaseLeasesFreq == 0 {
		return errConfigTakeAndReleaseLeasesFreqMustBeGreaterThanZero
	}
	return nil
}

type Consumer interface {
	ProcessRecords(shardID string, records []*kinesis.Record) error
	Shutdown(*Checkpointer) error
}

type KinesisRecord struct {
	ApproximateArrivalTimestamp int64
	Data                        string
	PartitionKey                string
	SequenceNumber              string
}

type Checkpointer struct {
}

func (c *Checkpointer) CheckpointAll() error {
	return nil
}

func (c *Checkpointer) CheckpointSequence(seq string) error {
	return nil
}
