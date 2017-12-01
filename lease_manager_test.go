package kinesis

import (
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
	t.Run("", func(t *testing.T) {
		repo := &LeaseRepoMock{}
		l := &leaseManager{
			leaseRepo: repo,
			leaseTTL:  time.Minute,
			logger:    newTestLogger(t),
			workerID:  "worker",
			allLeases: map[string]*lease{
				"123": &lease{
					Key:   "123",
					Owner: "worker",
				},
				"456": &lease{ //expired
					Key:   "456",
					Owner: "worker",
					//lastCounterIncrement: time.Now().Add(time.Minute),
				},
			},
			allLeasesMu: sync.Mutex{},
		}
		if errs := l.RenewLeases(); len(errs) > 0 {
			t.Errorf("leaseManager.RenewLeases() = %v, expected no errors", errs)
		}

		if len(repo.renewLeaseCalls) != 1 {
			t.Fatalf("leaseManager.RenewLeases(), expected 1 calls, got %d", len(repo.renewLeaseCalls))
		}

		if repo.renewLeaseCalls[0].key != "123" {
			t.Errorf("leaseManager.RenewLeases(), expected lease '%s' to be renewed", repo.renewLeaseCalls[0].key)
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
