package kinesis

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type testLogger struct {
	*testing.T
	suppressOutput bool
}

func newTestLogger(t *testing.T) *testLogger { return &testLogger{T: t} }

func (l *testLogger) Logf(msg string, args ...interface{}) {
	if l.suppressOutput {
		return
	}
	l.T.Logf(msg, args...)
}
func (l *testLogger) Errorf(msg string, args ...interface{}) {
	if l.suppressOutput {
		return
	}
	l.T.Logf("ERROR: "+msg, args...)
}

func (l *testLogger) quiet() *testLogger {
	l.suppressOutput = true
	return l
}

func Test_leaseSyncer_Run(t *testing.T) {
	t.Run("shutdown when shutdown chan is closed", func(t *testing.T) {
		l := &leaseSyncer{
			leaseRepo:     &LeaseRepoStub{},
			logger:        newTestLogger(t),
			kinesis:       &KinesisProxyStub{},
			streamName:    "",
			leaseSyncFreq: time.Second,
		}
		sh := make(chan struct{})
		exited := false
		go func() {
			l.Run(sh)
			exited = true
		}()
		close(sh)
		time.Sleep(time.Millisecond)
		if !exited {
			t.Error("syncer did not shutdown")
		}
	})

	t.Run("describe stream is called according to freq", func(t *testing.T) {
		kMock := &KinesisProxyMock{err: errors.New("")}
		l := &leaseSyncer{
			leaseRepo:     &LeaseRepoStub{},
			logger:        newTestLogger(t).quiet(),
			kinesis:       kMock,
			streamName:    "",
			leaseSyncFreq: time.Millisecond * 10,
		}
		sh := make(chan struct{})
		exited := false
		go func() {
			l.Run(sh)
			exited = true
		}()
		time.Sleep(time.Second)
		close(sh)
		time.Sleep(time.Second)
		if !exited {
			t.Error("syncer did not shutdown")
		}
		//the timing isn't 100% deterministic so as long as we get 98, that's close enough
		if kMock.describeStreamCalls < 98 {
			t.Errorf("expected at least 98 calls to DescribeStream, got %d", kMock.describeStreamCalls)
		}
	})

}

type KinesisProxyMock struct {
	*KinesisProxyStub
	err                 error
	describeStreamCalls int
}

func (k *KinesisProxyMock) DescribeStream(*kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	k.describeStreamCalls++
	return nil, k.err

}

func Test_leaseSyncer_syncLeases(t *testing.T) {
	t.Run("if stream is inactive, error is returned", func(t *testing.T) {
		l := &leaseSyncer{
			logger: newTestLogger(t),
			kinesis: &KinesisProxyStub{describeStreamOut: &kinesis.DescribeStreamOutput{
				StreamDescription: &kinesis.StreamDescription{
					StreamStatus: aws.String("CREATING"),
				},
			}},
		}
		err := l.syncLeases()
		if _, ok := err.(errStreamNotActive); !ok {
			t.Errorf("leaseSyncer.syncLeases() error = %v, wantErr errStreaNotActive", err)
		}
	})

	t.Run("if getleases returns error, it's returned", func(t *testing.T) {
		expectedErr := errors.New("an error")
		l := &leaseSyncer{
			logger: newTestLogger(t),
			leaseRepo: &LeaseRepoStub{
				err: expectedErr,
			},
			kinesis: &KinesisProxyStub{describeStreamOut: &kinesis.DescribeStreamOutput{
				StreamDescription: &kinesis.StreamDescription{
					StreamStatus: aws.String("ACTIVE"),
				},
			}},
		}
		if err := l.syncLeases(); err != expectedErr {
			t.Errorf("leaseSyncer.syncLeases() error = %v, wantErr %v", err, expectedErr)
		}
	})

	t.Run("create lease if it exists in kinesis, but not the repo", func(t *testing.T) {
		leaseRepo := &LeaseRepoMock{
			LeaseRepoStub: &LeaseRepoStub{
				leases: []*lease{
					{Key: "123"},
				},
			},
		}
		l := &leaseSyncer{
			logger:    newTestLogger(t),
			leaseRepo: leaseRepo,
			kinesis: &KinesisProxyStub{
				describeStreamOut: &kinesis.DescribeStreamOutput{
					StreamDescription: &kinesis.StreamDescription{
						Shards: []*kinesis.Shard{
							{ShardId: aws.String("123")}, //exists
							{ShardId: aws.String("456")}, //doesn't exist
						},
						StreamStatus: aws.String("ACTIVE"),
					},
				},
			},
		}
		if err := l.syncLeases(); err != nil {
			t.Errorf("leaseSyncer.syncLeases() error = %v, wantErr %v", err, nil)
		}
		if len(leaseRepo.createLeaseCalls) != 1 {
			t.Errorf("leaseSyncer.syncLeases() expected 1 create lease call, got %d", len(leaseRepo.createLeaseCalls))
		}
		if leaseRepo.createLeaseCalls[0].Key != "456" {
			t.Errorf("leaseSyncer.syncLeases() expected shard key to be '456', got %d", leaseRepo.createLeaseCalls[0].Key)
		}
	})
}
