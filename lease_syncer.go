package kinesis

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type errStreamNotActive string

func (e errStreamNotActive) Error() string {
	return "lease manager: could not sync leases, stream not active, stream status = '" + string(e) + "'"
}

// LeaseSyncer is responsible for inserting new leases found in the Kinesis API into the LeaseRepository
// as well as possibly cleaning up old leases
type leaseSyncer struct {
	leaseRepo LeaseRepository
	logger    Logger
	kinesis   kinesisProxy

	streamName              string
	leaseSyncFreq           time.Duration
	initialPositionInStream InitialPositionInStream
}

//checks for new shards and creates leases in the LeaseRepository for them
func (l *leaseSyncer) Run(shutdown chan struct{}) {
	tick := time.NewTicker(l.leaseSyncFreq)
	for {
		select {
		case <-shutdown:
			l.logger.Logf("syncer: shutting down shard syncer")
			return
		case <-tick.C:
			err := l.syncLeases()
			if err != nil {
				l.logger.Logf("lease manager: error syncing leases: %s", err)
			}
		}
	}
}

// Creates new leases for shards that aren't currently in the repository
// Once a new lease is created, it can be taken by a worker
func (l *leaseSyncer) syncLeases() error {
	out, err := l.kinesis.DescribeStream(&kinesis.DescribeStreamInput{
		Limit:      aws.Int64(10000),
		StreamName: aws.String(l.streamName),
	})
	if err != nil {
		return err
	}

	if out.StreamDescription.StreamStatus == nil || *out.StreamDescription.StreamStatus != "ACTIVE" {
		return errStreamNotActive(*out.StreamDescription.StreamStatus)
	}

	repoLeases, err := l.leaseRepo.GetLeases()
	if err != nil {
		return err
	}
	//key by shard
	m := make(map[string]*lease)
	for _, lease := range repoLeases {
		m[lease.Key] = lease
	}

	var errs Errors
	for _, s := range out.StreamDescription.Shards {
		if s.ShardId == nil {
			//shouldn't happen
			continue
		}
		//TODO: clean up expired leases
		if _, ok := m[*s.ShardId]; ok {
			continue
		}
		lease := &lease{
			Key:           *s.ShardId,
			Counter:       0,
			Owner:         "",
			ParentShardId: aws.StringValue(s.ParentShardId),
			Checkpoint:    l.initialPositionInStream.String(),
		}
		err := l.leaseRepo.CreateLeaseIfNotExists(lease)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}
