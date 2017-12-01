package kinesis

import "github.com/aws/aws-sdk-go/service/kinesis"

type KinesisProxyStub struct {
	getShardOut       *kinesis.GetShardIteratorOutput
	getRecordsOut     *kinesis.GetRecordsOutput
	describeStreamOut *kinesis.DescribeStreamOutput
	err               error
}

func (k *KinesisProxyStub) GetShardIterator(in *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	return k.getShardOut, k.err
}
func (k *KinesisProxyStub) GetRecords(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	return k.getRecordsOut, k.err
}
func (k *KinesisProxyStub) DescribeStream(*kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	return k.describeStreamOut, k.err
}
