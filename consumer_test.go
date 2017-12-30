package kinesis

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type KinesisProxyStub struct {
	kinesisiface.KinesisAPI

	getShardOut       *kinesis.GetShardIteratorOutput
	getRecordsOut     *kinesis.GetRecordsOutput
	describeStreamOut *kinesis.DescribeStreamOutput
	err               error

	shardIteratorRecords map[string][]*kinesis.Record
}

func NewKinesisProxyStub() *KinesisProxyStub {
	return &KinesisProxyStub{
		shardIteratorRecords: map[string][]*kinesis.Record{},
	}
}

func (k *KinesisProxyStub) WithShards(v int) *KinesisProxyStub {
	k.describeStreamOut = &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			StreamStatus:         aws.String("ACTIVE"),
			HasMoreShards:        aws.Bool(false),
			RetentionPeriodHours: aws.Int64(24),
			Shards:               []*kinesis.Shard{},
		},
	}
	for i := 1; i <= v; i++ {
		k.describeStreamOut.StreamDescription.Shards = append(
			k.describeStreamOut.StreamDescription.Shards,
			&kinesis.Shard{
				ShardId: aws.String(strconv.Itoa(i)),
			},
		)
	}
	return k
}

func (k *KinesisProxyStub) WithShardRecords(shardID string, records []*kinesis.Record) *KinesisProxyStub {
	k.shardIteratorRecords[shardID] = records
	return k
}

func (k *KinesisProxyStub) GetShardIterator(in *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	return &kinesis.GetShardIteratorOutput{
		ShardIterator: aws.String(encodeShardIterator(*in.ShardId, 0)),
	}, k.err
}

func encodeShardIterator(shardID string, recordIterator int64) string {
	return fmt.Sprintf("%s %d", shardID, recordIterator)
}

func decodeShardIterator(v string) (shardID string, iterator int64) {
	_, err := fmt.Sscanf(v, "%s %d", &shardID, &iterator)
	if err != nil {
		panic(err)
	}
	return
}

func (k *KinesisProxyStub) GetRecords(in *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	shardID, iterator := decodeShardIterator(*in.ShardIterator)
	lastRecord := iterator + *in.Limit
	if int(lastRecord) > len(k.shardIteratorRecords[shardID]) {
		lastRecord = int64(len(k.shardIteratorRecords[shardID]))
	}
	return &kinesis.GetRecordsOutput{
		NextShardIterator: aws.String(encodeShardIterator(shardID, lastRecord)),
		Records:           k.shardIteratorRecords[shardID][iterator:lastRecord],
	}, k.err
}
func (k *KinesisProxyStub) DescribeStream(*kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	return k.describeStreamOut, k.err
}
