package kinesis

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type leaseRepo interface {
	GetLeases() ([]*lease, error)
	CreateLeaseIfNotExists(*lease) error

	//takes a lease
	//owner changed to passed in value
	TakeLease(key, owner string) error

	//drop lease
	//only applied if owner passed in is equal to owner in data store
	DropLease(key, owner string) error

	//updates the checkpoint for a lease
	//the counter should be incremented and checkpoint updated
	//the value of the lease owner before updating should be equal to the passed in owner
	//if upon persisting, the counter or owner is not what is expected, the LostLeaseError should be returned
	UpdateCheckpoint(key, checkpoint, owner string, counter int64) error

	//renews lease
	//the counter that is passed in should be incremented and persisted
	//the value of the lease owner before updating should be equal to the passed in owner
	//if upon persisting, the counter or owner is not what is expected, the LostLeaseError should be returned
	RenewLease(key, owner string, counter int64) error
}

type LostLeaseError struct {
	ShardID string
	Reason  string
	Err     error
}

func newLostLeaseError(shardID, reason string, err error) LostLeaseError {
	return LostLeaseError{
		ShardID: shardID,
		Reason:  reason,
		Err:     err,
	}
}

func (l LostLeaseError) Error() string {
	return "kinesis: lost lease shardId=" + l.ShardID + " reason=" + l.Reason + " err=" + l.Err.Error()
}

type DynamoLeaseRepository struct {
	c     *dynamodb.DynamoDB
	table string
}

func NewDynamoLeaseRepository(c *dynamodb.DynamoDB, table string) *DynamoLeaseRepository {
	return &DynamoLeaseRepository{
		c:     c,
		table: table,
	}
}

//this will check that the necessary table exists in DynamoDB and create it if not
func (d *DynamoLeaseRepository) Init() error {
	//TODO: check for table, create if not exists
	return nil
}

//TODO: all this stuff
func (d *DynamoLeaseRepository) GetLeases() ([]*lease, error)                      { return nil, nil }
func (d *DynamoLeaseRepository) CreateLeaseIfNotExists(*lease) error               { return nil }
func (d *DynamoLeaseRepository) TakeLease(key, owner string) error                 { return nil }
func (d *DynamoLeaseRepository) DropLease(key, owner string) error                 { return nil }
func (d *DynamoLeaseRepository) RenewLease(key, owner string, counter int64) error { return nil }
func (d *DynamoLeaseRepository) UpdateCheckpoint(key, checkpoint, owner string, counter int64) error {
	//TODO: add logic for validating iterator via Kinesis API
	//e.g.validateWithGetIterator
	return nil
}

type LeaseCache struct {
	leaseRepo

	cacheTTL time.Duration

	leases []*lease
	mu     sync.Mutex
	exp    time.Time
}

func (c *LeaseCache) GetLeases() ([]*lease, error) {
	//if the cached leases aren't expired, return them
	if c.exp.Before(time.Now()) {
		leases, err := c.leaseRepo.GetLeases()
		if err != nil {
			return leases, err
		}
		//TODO: make thread safe
		c.leases = leases
		c.exp = time.Now().Add(c.cacheTTL)
	}
	return c.leases, nil
}

func (c *LeaseCache) CreateLeaseIfNotExists(l *lease) error {
	//if creation is successful, add to lease cache
	err := c.leaseRepo.CreateLeaseIfNotExists(l)
	if err != nil {
		return err
	}
	//need to make sure it doesn't exist in case it's been inserted since we created it
	c.addToLeaseCacheIfNotExists(l)
	return nil
}

func (c *LeaseCache) addToLeaseCacheIfNotExists(l *lease) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, lc := range c.leases {
		if lc.Key == l.Key {
			return
		}
	}
	c.leases = append(c.leases, l)
}

func (c *LeaseCache) TakeLease(key, owner string) error {
	err := c.leaseRepo.TakeLease(key, owner)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	//update lease owner in cache
	for _, lc := range c.leases {
		if lc.Key == key {
			lc.Owner = owner
			return nil
		}
	}

	return nil
}
func (c *LeaseCache) UpdateCheckpoint(key, checkpoint, owner string, counter int64) error {
	//if successful, update lease in cache
	err := c.leaseRepo.UpdateCheckpoint(key, checkpoint, owner, counter)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	//update lease owner in cache
	for _, lc := range c.leases {
		if lc.Key == key {
			lc.Owner = owner
			lc.Checkpoint = checkpoint
			lc.Counter = counter
			return nil
		}
	}
	//a lease was updated that we don't have an entry for, expire the cache

	return nil
}
