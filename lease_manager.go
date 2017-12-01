package kinesis

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const CheckpointShardClosed = "SHARD_END"

type ShardLease struct {
	StreamName string
	ShardID    string
	Checkpoint string
}
type lease struct {
	Key                  string
	Owner                string
	Counter              int64
	Checkpoint           string
	ParentShardId        string
	lastCounterIncrement time.Time
}

func (s *lease) IsClosed() bool {
	return s.Checkpoint == CheckpointShardClosed
}

func (s *lease) IsExpired(ttl time.Duration, now time.Time) bool {
	if s.lastCounterIncrement.IsZero() {
		return true
	}
	return s.lastCounterIncrement.Add(ttl).Before(now)
}

func (s ShardLease) getShardIteratorInput() *kinesis.GetShardIteratorInput {
	itType := aws.String("")
	seqNum := aws.String("")
	return &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(s.ShardID),
		ShardIteratorType:      itType,
		StreamName:             aws.String(s.StreamName),
		StartingSequenceNumber: seqNum,
	}
}

// leaseManager is responsible for obtaining leases for the worker and notifying the worker when a lease is lost
type leaseManager struct {
	leaseRepo
	logger      Logger
	leaseSyncer *leaseSyncer

	workerID       string
	streamName     string
	leaseTTL       time.Duration
	leaseSyncFreq  time.Duration
	leaseRenewFreq time.Duration

	allLeases   map[string]*lease
	allLeasesMu sync.Mutex

	shutdown chan struct{}
}

func newLeaseManager(r leaseRepo, k *kinesis.Kinesis, config Config) *leaseManager {
	lm := &leaseManager{
		leaseRepo: r,
		logger:    DefaultLogger,
		leaseSyncer: &leaseSyncer{
			leaseRepo:               r,
			logger:                  DefaultLogger,
			kinesis:                 k,
			streamName:              config.StreamName,
			leaseSyncFreq:           config.ShardSyncFrequency,
			initialPositionInStream: config.InitialPositionInStream,
		},
		workerID:  config.WorkerID,
		leaseTTL:  config.LeaseTTL,
		allLeases: map[string]*lease{},
	}
	return lm
}

func (l *leaseManager) Start() (takenLeases, lostLeases chan ShardLease) {
	takenLeases = make(chan ShardLease)
	lostLeases = make(chan ShardLease)

	l.shutdown = make(chan struct{})
	go l.runLeaseRenewer(lostLeases) //renews currently held leases
	go l.runLeaseWatcher()           //syncs leases in repo with l.allLeases
	go l.leaseSyncer.Run(l.shutdown) //syncs leases in Kinesis API with leases in repo

	//do stuff to get leases or whatever
	return takenLeases, lostLeases
}

func (l *leaseManager) runLeaseRenewer(lostLeases chan<- ShardLease) {
	ticker := time.NewTicker(l.leaseRenewFreq)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			errs := l.RenewLeases()
			for _, err := range errs {
				if e, ok := err.(LostLeaseError); ok {
					lostLeases <- ShardLease{StreamName: l.streamName, ShardID: e.ShardID}
				}
				l.logger.Errorf("lease renewer: %v", err)
			}
		case <-l.shutdown:
			return
		}
	}
}

func (l *leaseManager) RenewLeases() []error {
	//get currently owned leases
	leases := l.copyOfAllLeases()
	var errs []error
	for _, lease := range leases.ownedBy(l.workerID) {
		//don't renew lost leases
		if lease.IsExpired(l.leaseTTL, time.Now()) {
			l.logger.Errorf("lease manager: lease lost when attempting to renew, shardID: %s", lease.Key)
			errs = append(errs, newLostLeaseError(lease.Key, "lease expired when attempting renewal", nil))
			continue
		}
		err := l.leaseRepo.RenewLease(lease.Key, lease.Owner, lease.Counter)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (l *leaseManager) runLeaseWatcher() {
	ticker := time.NewTicker(l.leaseSyncFreq)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := l.UpdateLeases()
			if err != nil {
				l.logger.Errorf("lease syncer: %v", err)
			}
		case <-l.shutdown:
			return
		}
	}
}

//Returns new leases to process and leases that we should drop
func (l *leaseManager) TakeAndReleaseLeases() error {
	leasesToTake, leasesToGiveUp := l.determineLeaseChanges()
	for _, lease := range leasesToTake {
		if err := l.TakeLease(lease.Key, l.workerID); err != nil {
			return err
		}
	}
	for _, lease := range leasesToGiveUp {
		if err := l.DropLease(lease.Key, l.workerID); err != nil {
			return err
		}
	}
	l.logger.Logf("took %d leases, dropped %d leases", len(leasesToTake), len(leasesToGiveUp))
	return nil
}

func (l *leaseManager) copyOfAllLeases() *leases {
	l.allLeasesMu.Lock()
	defer l.allLeasesMu.Unlock()
	leases := newAllLeases()
	for _, ls := range l.allLeases {
		cp := *ls
		leases.add(&cp)
	}
	return leases
}

type leases struct {
	m      map[string]*lease
	owners map[string][]*lease
}

func newAllLeases() *leases {
	return &leases{
		m:      map[string]*lease{},
		owners: map[string][]*lease{},
	}
}

func (a *leases) add(l *lease) {
	a.m[l.Key] = l
	a.owners[l.Owner] = append(a.owners[l.Owner], l)
}

func (a *leases) getByShardId(shardID string) (l *lease, ok bool) {
	l, ok = a.m[shardID]
	return
}

func (a *leases) numLeases() int {
	return len(a.m)
}

func (a *leases) numWorkers() int {
	return len(a.owners)
}

func (a *leases) hasWorker(owner string) bool {
	if _, ok := a.owners[owner]; ok {
		return true
	}
	return false
}

func (a *leases) ownedBy(owner string) []*lease {
	return a.owners[owner]
}

//
//go through all leases and grab any that
// 1. Are expired
// 2. Doesn't have an owner
// 3. Do not take a lease if it's parent shard is not complete (SHARD_END)
// 4. You can steal 1 per call to this function for purposes of load balancing
func (l *leaseManager) determineLeaseChanges() (leasesToTake, leasesToDrop []*lease) {
	allLeases := l.copyOfAllLeases()
	numWorkers := allLeases.numWorkers()

	//if we're not in the count of all workers, add one
	if !allLeases.hasWorker(l.workerID) {
		numWorkers++
	}
	targetLeases := allLeases.numLeases() / numWorkers
	if allLeases.numLeases()%numWorkers > 0 {
		targetLeases++
	}

	ownedLeases := allLeases.ownedBy(l.workerID)
	if len(ownedLeases) == targetLeases {
		//we have the right amount of leases, no changes
		return
	}
	if len(ownedLeases) > targetLeases {
		//we have too many leases, drop the difference
		leasesToDrop = ownedLeases[0 : len(ownedLeases)-targetLeases]
		//the leases that are dropped are kind of randomized since the lease list is in a map,
		// which is what we want
		return

	}

	//we need more leases, get list of potential leases to take
	for _, lease := range allLeases.m {
		if len(leasesToTake) == targetLeases {
			break
		}
		//we already have this lease, skip
		if lease.Owner == l.workerID {
			continue
		}
		//don't take a lease that had a parent that is still being read from
		if lease.ParentShardId != "" {
			parentLease, ok := allLeases.getByShardId(lease.ParentShardId)
			if ok && parentLease.IsClosed() {
				continue
			}
		}
		if lease.IsExpired(l.leaseTTL, time.Now()) || lease.Owner == "" {
			leasesToTake = append(leasesToTake, lease)
		}
	}

	//should we steal one?
	if len(leasesToTake) == 0 && targetLeases > 0 {
		//take one from another worker that has numLeases == target
		for owner, leases := range allLeases.owners {
			if owner == l.workerID {
				continue
			}
			if len(leases) == targetLeases {
				leasesToTake = append(leasesToTake, leases[0])
				break
			}
		}
	}

	return
}

//fetches the list of leases from the lease repository and updates
//l.allLeases with the info
func (l *leaseManager) UpdateLeases() error {
	leases, err := l.GetLeases()
	if err != nil {
		return err
	}

	l.allLeasesMu.Lock()
	defer l.allLeasesMu.Unlock()
	for _, lease := range leases {
		if oldLease, ok := l.allLeases[lease.Key]; ok {
			if lease.Counter > oldLease.Counter {
				lease.lastCounterIncrement = time.Now()
			}
			l.allLeases[lease.Key] = lease
		} else {
			//we haven't seen this lease before, add it
			lease.lastCounterIncrement = time.Now()
			l.allLeases[lease.Key] = lease
		}
	}
	//TODO: remove leases that don't exist anymore
	return nil
}

type InitialPositionInStream struct {
	position  string
	timestamp time.Time
}

func (i InitialPositionInStream) String() string {
	return i.position
}

func NewInitialPositionInStreamTrimHorizon() InitialPositionInStream {
	return InitialPositionInStream{position: "TRIM_HORIZON"}
}
func NewInitialPositionInStreamLatest() InitialPositionInStream {
	return InitialPositionInStream{position: "LATEST"}
}
func NewInitialPositionInStreamAtTimestamp(t time.Time) InitialPositionInStream {
	return InitialPositionInStream{
		position:  "AT_TIMESTAMP",
		timestamp: t,
	}
}
