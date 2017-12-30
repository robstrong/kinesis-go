package kinesis

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func Test_leaseManager_runLeaseRenewer(t *testing.T) {
	t.Run("closing shutdown chan, end func", func(t *testing.T) {
		sh := make(chan struct{})
		l := &leaseManager{
			shutdown:       sh,
			leaseRenewFreq: time.Second,
		}
		var exited bool
		go func() {
			l.runLeaseRenewer(nil)
			exited = true
		}()
		close(sh)
		time.Sleep(time.Millisecond * 100)
		if !exited {
			t.Errorf("expected renewer to shutdown")
		}
	})
}

func Test_leaseManager_RenewLeases(t *testing.T) {
	t.Run("owned leases that are not expired are renewed", func(t *testing.T) {
		repo := NewLeaseRepoMock()
		l := &leaseManager{
			leaseRepo: repo,
			leaseTTL:  time.Minute,
			logger:    newTestLogger(t).quiet(),
			workerID:  "worker",
			allLeases: map[string]*lease{
				"123": &lease{
					Key:   "123",
					Owner: "worker",
				},
				"456": &lease{ //expired
					Key:                  "456",
					Owner:                "worker",
					lastCounterIncrement: time.Now().Add(time.Minute * -1),
				},
				"789": &lease{ //not owned
					Key:   "456",
					Owner: "worker2",
				},
			},
			allLeasesMu: sync.Mutex{},
		}
		if errs := l.RenewLeases(); len(errs) != 1 { //one err for the expired lease
			t.Errorf("leaseManager.RenewLeases() = %v, expected 1 error", errs)
		}

		if len(repo.renewLeaseCalls) != 1 { //one renewal for the owned, non-expired lease
			t.Fatalf("leaseManager.RenewLeases(), expected 1 calls, got %d", len(repo.renewLeaseCalls))
		}

		if repo.renewLeaseCalls[0].key != "123" {
			t.Errorf("leaseManager.RenewLeases(), expected lease '%s' to be renewed", repo.renewLeaseCalls[0].key)
		}
	})

	t.Run("errs returned by repo.RenewLease() is returned by RenewLeases()", func(t *testing.T) {
		repo := NewLeaseRepoMock()
		expectedErr := errors.New("test")
		repo.err = expectedErr
		l := &leaseManager{
			leaseRepo: repo,
			leaseTTL:  time.Minute,
			logger:    newTestLogger(t).quiet(),
			workerID:  "worker",
			allLeases: map[string]*lease{
				"123": &lease{
					Key:   "123",
					Owner: "worker",
				},
			},
			allLeasesMu: sync.Mutex{},
		}
		errs := l.RenewLeases()
		if len(errs) != 1 {
			t.Fatalf("leaseManager.RenewLeases() = %v, expected 1 error", errs)
		}

		if errs[0] != expectedErr {
			t.Errorf("leaseManager.RenewLeases(), did not get expected err, got: %v", errs[0])
		}
	})
}

func Test_lease_IsExpired(t *testing.T) {
	tests := []struct {
		name                 string
		now                  time.Time
		lastCounterIncrement time.Time
		ttl                  time.Duration
		want                 bool
	}{
		{
			name:                 "same time is not expired",
			now:                  time.Date(2017, 11, 30, 12, 13, 14, 15, time.UTC),
			lastCounterIncrement: time.Date(2017, 11, 30, 12, 13, 14, 15, time.UTC),
			ttl:                  time.Minute,
			want:                 false,
		},
		{
			name:                 "equal to ttl is not expired",
			now:                  time.Date(2017, 11, 30, 12, 13, 14, 15, time.UTC),
			lastCounterIncrement: time.Date(2017, 11, 30, 12, 12, 14, 15, time.UTC),
			ttl:                  time.Minute,
			want:                 false,
		},
		{
			name:                 "ns passed ttl is expired",
			now:                  time.Date(2017, 11, 30, 12, 13, 14, 16, time.UTC),
			lastCounterIncrement: time.Date(2017, 11, 30, 12, 12, 14, 15, time.UTC),
			ttl:                  time.Minute,
			want:                 true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &lease{
				lastCounterIncrement: tt.lastCounterIncrement,
			}
			if got := s.IsExpired(tt.ttl, tt.now); got != tt.want {
				t.Errorf("lease.IsExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_leaseManager_determineLeaseChanges(t *testing.T) {
	tests := []struct {
		name      string
		workerID  string
		leaseTTL  time.Duration
		allLeases map[string]*lease
		//since the leases that will be taken/dropped is non-deterministic,
		//we specify the number of takes/drops and a map of lease keys that can be taken/dropped
		expectedTakeCount int
		expectedTakeKeys  []string
		expectedDropCount int
		expectedDropKeys  []string
	}{
		{
			name:              "if no leases, don't take any, don't drop any",
			leaseTTL:          time.Minute,
			workerID:          "worker",
			allLeases:         map[string]*lease{},
			expectedTakeCount: 0,
			expectedDropCount: 0,
		},
		{
			name:     "one lease available without owner, need one lease, should take it",
			leaseTTL: time.Minute,
			workerID: "worker",
			allLeases: map[string]*lease{
				"1": &lease{
					Key: "1",
				},
			},
			expectedTakeCount: 1,
			expectedTakeKeys:  []string{"1"},
			expectedDropCount: 0,
		},
		{
			name:     "one lease that is already owned, no changes",
			workerID: "worker",
			leaseTTL: time.Minute,
			allLeases: map[string]*lease{
				"1": &lease{
					Key:   "1",
					Owner: "worker",
				},
			},
			expectedTakeCount: 0,
			expectedDropCount: 0,
		},
		{
			name:     "one lease that is already owned by another worker, one lease available, take available",
			workerID: "worker",
			leaseTTL: time.Minute,
			allLeases: map[string]*lease{
				"1": &lease{
					Key:   "1",
					Owner: "worker2",
				},
				"2": &lease{
					Key:   "2",
					Owner: "",
				},
			},
			expectedTakeCount: 1,
			expectedTakeKeys:  []string{"2"},
			expectedDropCount: 0,
		},
		{
			name:     "one lease that is already owned by another worker, should not take it",
			workerID: "worker",
			leaseTTL: time.Minute,
			allLeases: map[string]*lease{
				"1": &lease{
					Key:   "1",
					Owner: "worker2",
				},
			},
			expectedTakeCount: 0,
			expectedDropCount: 0,
		},
		{
			name:     "three leases, one already owned, should take the two others",
			workerID: "worker",
			leaseTTL: time.Minute,
			allLeases: map[string]*lease{
				"1": {
					Key:   "1",
					Owner: "worker",
				},
				"2": {Key: "2"},
				"3": {Key: "3"},
			},
			expectedTakeCount: 2,
			expectedTakeKeys:  []string{"2", "3"},
			expectedDropCount: 0,
		},
		{
			name:     "four leases, three are owned, the other is owned by another worker, should drop one",
			workerID: "worker",
			leaseTTL: time.Minute,
			allLeases: map[string]*lease{
				"1": {
					Key:   "1",
					Owner: "worker",
				},
				"2": {Key: "2", Owner: "worker"},
				"3": {Key: "3", Owner: "worker"},
				"4": {Key: "4", Owner: "worker2"},
			},
			expectedTakeCount: 0,
			expectedDropCount: 1,
			expectedDropKeys:  []string{"1", "2", "3"},
		},
		{
			name:     "eight leases, 4 workers, 5 available leases, only take 2 leases",
			workerID: "worker",
			leaseTTL: time.Minute,
			allLeases: map[string]*lease{
				"1": {Key: "1", Owner: "worker1"},
				"2": {Key: "2", Owner: "worker2"},
				"3": {Key: "3", Owner: "worker3"},
				"4": {Key: "4", Owner: ""},
				"5": {Key: "5", Owner: ""},
				"6": {Key: "6", Owner: ""},
				"7": {Key: "7", Owner: ""},
				"8": {Key: "8", Owner: ""},
			},
			expectedTakeCount: 2,
			expectedTakeKeys:  []string{"4", "5", "6", "7", "8"},
			expectedDropCount: 0,
			expectedDropKeys:  nil,
		},
		{
			name:     "don't take leases that have an unfinished parent shard",
			workerID: "worker",
			leaseTTL: time.Minute,
			allLeases: map[string]*lease{
				"1": {Key: "1", Owner: "worker"},
				"2": {Key: "2", Owner: "worker1"},
				"3": {Key: "3", Owner: "worker1"},
				"4": {Key: "4", Owner: "", ParentShardID: "3"},
			},
			expectedTakeCount: 0,
			expectedDropCount: 0,
		},
		{
			name:     "another worker has too many leases, steal one",
			workerID: "worker",
			leaseTTL: time.Minute,
			allLeases: map[string]*lease{
				"1": {Key: "1", Owner: "worker1"},
				"2": {Key: "2", Owner: "worker1"},
				"3": {Key: "3", Owner: "worker1"},
			},
			expectedTakeCount: 1,
			expectedTakeKeys:  []string{"1", "2", "3"},
			expectedDropCount: 0,
		},
		{
			name:     "don't take a completed shard",
			workerID: "worker",
			leaseTTL: time.Minute,
			allLeases: map[string]*lease{
				"1": {Key: "1", Owner: "worker1"},
				"2": {Key: "2", Owner: "", Checkpoint: CheckpointShardClosed},
			},
			expectedTakeCount: 0,
			expectedDropCount: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &leaseManager{
				logger:      newTestLogger(t),
				workerID:    tt.workerID,
				leaseTTL:    tt.leaseTTL,
				allLeases:   tt.allLeases,
				allLeasesMu: sync.Mutex{},
			}
			take, drop := l.determineLeaseChanges()
			if tt.expectedTakeCount != len(take) {
				t.Errorf("leaseManager.determineLeaseChanges() expected %d taken leases, got %d", tt.expectedTakeCount, len(take))
			}
			if tt.expectedDropCount != len(drop) {
				t.Errorf("leaseManager.determineLeaseChanges() expected %d drops, got %d", tt.expectedDropCount, len(drop))
			}
			testIsSubsetOfLeaseKeys(t, take, tt.expectedTakeKeys)
			testIsSubsetOfLeaseKeys(t, drop, tt.expectedDropKeys)
		})
	}
}

func testIsSubsetOfLeaseKeys(t *testing.T, leases []*lease, keys []string) {
	exp := strSliceToMap(keys)
	for _, l := range leases {
		if _, ok := exp[l.Key]; !ok {
			t.Errorf("leaseManager.determineLeaseChanges() got unexpected lease, leaseKey=%s", l.Key)
		}
	}
}

func strSliceToMap(v []string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, k := range v {
		m[k] = struct{}{}
	}
	return m
}
