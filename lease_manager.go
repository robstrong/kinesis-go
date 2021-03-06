package kinesis

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const CheckpointShardClosed = "SHARD_END"

type ShardLease struct {
	StreamName string
	ShardID    string
}

type lease struct {
	Key                  string
	Owner                string
	Counter              int64
	Checkpoint           string
	ParentShardID        string
	lastCounterIncrement time.Time
}

func (s *lease) IsClosed() bool {
	return s.Checkpoint == CheckpointShardClosed
}

func (s *lease) IsExpired(ttl time.Duration, now time.Time) bool {
	if s.lastCounterIncrement.IsZero() {
		return false
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

	workerID              string
	streamName            string
	leaseTTL              time.Duration
	leaseSyncFromRepoFreq time.Duration
	leaseRenewFreq        time.Duration
	takeReleaseLeaseFreq  time.Duration

	allLeases   map[string]*lease
	allLeasesMu sync.Mutex

	shutdown chan struct{}
}

func newLeaseManager(r leaseRepo, k kinesisiface.KinesisAPI, config *Config) *leaseManager {
	lm := &leaseManager{
		leaseRepo: r,
		logger:    DefaultLogger,
		leaseSyncer: &leaseSyncer{
			leaseRepo:               r,
			logger:                  DefaultLogger,
			kinesis:                 k,
			streamName:              config.StreamName,
			syncFreq:                config.ShardSyncFromAPIFrequency,
			initialPositionInStream: config.InitialPositionInStream,
		},
		leaseRenewFreq:        config.LeaseRenewFrequency,
		leaseSyncFromRepoFreq: config.LeaseSyncFromRepoFreq,
		takeReleaseLeaseFreq:  config.TakeAndReleaseLeasesFreq,
		workerID:              config.WorkerID,
		leaseTTL:              config.LeaseTTL,
		allLeases:             map[string]*lease{},
	}
	return lm
}

func (l *leaseManager) Start() (takenLeases, lostLeases chan ShardLease) {
	takenLeases = make(chan ShardLease)
	lostLeases = make(chan ShardLease)

	l.shutdown = make(chan struct{})
	go l.runLeaseRenewer(lostLeases)                           //renews currently held leases
	go l.runLeaseWatcher()                                     //syncs leases in repo with l.allLeases
	go l.leaseSyncer.Run(l.shutdown)                           //syncs leases in Kinesis API with leases in repo
	go l.runTakeAndReleaseLeaseWorker(takenLeases, lostLeases) //determine if leases in l.allLeases should be taken/released

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
	ticker := time.NewTicker(l.leaseSyncFromRepoFreq)
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
func (l *leaseManager) runTakeAndReleaseLeaseWorker(takenLeases, lostLeases chan ShardLease) {
	ticker := time.NewTicker(l.takeReleaseLeaseFreq)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			leasesToTake, leasesToGiveUp := l.determineLeaseChanges()
			for _, lease := range leasesToTake {
				if err := l.TakeLease(lease.Key, l.workerID); err != nil {
					l.logger.Errorf("lease manager: error taking lease for shard %s: %v", lease.Key, err)
				}
				l.logger.Logf("lease manager: took lease %s", lease.Key)
				takenLeases <- ShardLease{
					StreamName: l.streamName,
					ShardID:    lease.Key,
				}
			}
			for _, lease := range leasesToGiveUp {
				if err := l.DropLease(lease.Key, l.workerID); err != nil {
					l.logger.Errorf("lease manager: error dropping lease for shard %s: %v", lease.Key, err)
				}
				l.logger.Logf("lease manager: dropped lease %s", lease.Key)
				lostLeases <- ShardLease{
					StreamName: l.streamName,
					ShardID:    lease.Key,
				}
			}
		case <-l.shutdown:
			return
		}
	}
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
	var oldOwner string
	if oldLease, ok := a.m[l.Key]; ok {
		oldOwner = oldLease.Owner
	}
	//if owner changed, remove lease from previous owner
	if l.Owner != oldOwner && oldOwner != "" {
		for i, lease := range a.owners[oldOwner] {
			if lease.Key == l.Key {
				a.owners[oldOwner] = append(a.owners[oldOwner][:i], a.owners[oldOwner][i+1:]...)
				break
			}
		}
	}
	a.m[l.Key] = l
	if l.Owner != "" {
		a.owners[l.Owner] = append(a.owners[l.Owner], l)
	}
}

func (a *leases) getByShardId(shardID string) (l *lease, ok bool) {
	l, ok = a.m[shardID]
	return
}

// Returns the number of shards that are ready to be read from
// This does not include shards that are not ACTIVE or shards which
// have a parent that is not in a CLOSED state
func (a *leases) activeLeases() int {
	count := 0
	for _, l := range a.m {
		if l.IsClosed() {
			continue
		}
		if l.ParentShardID != "" {
			if parentLease, ok := a.m[l.ParentShardID]; ok {
				if !parentLease.IsClosed() {
					continue
				}
			}
		}
		count++
	}
	return count
}

func (a *leases) length() int {
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
	l.logger.Logf("total workers: %d", numWorkers)
	l.logger.Logf("total leases/active leases: %d/%d", allLeases.length(), allLeases.activeLeases())

	targetLeases := allLeases.activeLeases() / numWorkers
	if allLeases.activeLeases()%numWorkers > 0 {
		targetLeases++
	}
	l.logger.Logf("target leases: %d", targetLeases)

	ownedLeases := allLeases.ownedBy(l.workerID)
	l.logger.Logf("owned leases: %d", len(ownedLeases))

	if len(ownedLeases) == targetLeases {
		//we have the right amount of leases, no changes
		return
	}
	if len(ownedLeases) > targetLeases {
		//we have too many leases, drop the difference
		leasesToDrop = ownedLeases[:len(ownedLeases)-targetLeases]
		//the leases that are dropped are kind of randomized since the lease list is in a map,
		// which is what we want
		return

	}

	//we need more leases, get list of potential leases to take
	for _, lease := range allLeases.m {
		if len(leasesToTake) == targetLeases {
			break
		}
		//we already have this lease or the shard is complete, skip
		if lease.Owner == l.workerID || lease.IsClosed() {
			continue
		}

		//don't take a lease that had a parent that is still being read from
		if lease.ParentShardID != "" {
			parentLease, ok := allLeases.getByShardId(lease.ParentShardID)
			if ok && !parentLease.IsClosed() {
				continue
			}
		}
		if lease.IsExpired(l.leaseTTL, time.Now()) || lease.Owner == "" {
			leasesToTake = append(leasesToTake, lease)
		}
	}

	//should we steal one?
	if len(leasesToTake) == 0 && targetLeases > 0 {
		//take one from another worker that has numLeases > target
		for owner, leases := range allLeases.owners {
			if owner == l.workerID {
				continue
			}
			if len(leases) > targetLeases {
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
			l.logger.Logf("lease manager: adding lease %q to local cache", lease.Key)
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
