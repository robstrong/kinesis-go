package kinesis

import "errors"

type LeaseRepoMock struct {
	createLeaseCalls []*lease
	renewLeaseCalls  []struct {
		key     string
		owner   string
		counter int64
	}

	err    error
	leases map[string]*lease
}

func NewLeaseRepoMock() *LeaseRepoMock {
	return &LeaseRepoMock{
		leases: map[string]*lease{},
	}
}

func (d *LeaseRepoMock) InitRepository() error {
	return d.err
}
func (d *LeaseRepoMock) CreateLeaseIfNotExists(l *lease) error {
	d.createLeaseCalls = append(d.createLeaseCalls, l)
	d.leases[l.Key] = l
	return d.err
}

func (d *LeaseRepoMock) RenewLease(key, owner string, counter int64) error {
	d.renewLeaseCalls = append(d.renewLeaseCalls, struct {
		key     string
		owner   string
		counter int64
	}{key, owner, counter})
	if d.err != nil {
		return d.err
	}
	if _, ok := d.leases[key]; !ok {
		return errors.New("lease not found: " + key)
	}

	d.leases[key].Owner = owner
	d.leases[key].Counter++
	return nil
}
func (d *LeaseRepoMock) GetLeases() ([]*lease, error) {
	leases := make([]*lease, len(d.leases))
	i := 0
	for _, l := range d.leases {
		leases[i] = l
		i++
	}
	return leases, d.err
}
func (d *LeaseRepoMock) TakeLease(key, owner string) error {
	d.leases[key].Owner = owner
	d.leases[key].Counter++
	return d.err
}
func (d *LeaseRepoMock) DropLease(key, owner string) error {
	d.leases[key].Owner = owner
	d.leases[key].Counter++
	return d.err
}
func (d *LeaseRepoMock) UpdateCheckpoint(key, checkpoint, owner string, counter int64) error {
	return d.err
}

type LeaseRepoStub struct {
	leases []*lease
	err    error
}

func (d *LeaseRepoStub) GetLeases() ([]*lease, error) {
	return d.leases, d.err
}
func (d *LeaseRepoStub) InitRepository() error {
	return d.err
}
func (d *LeaseRepoStub) CreateLeaseIfNotExists(*lease) error {
	return d.err
}
func (d *LeaseRepoStub) TakeLease(key, owner string) error {
	return d.err
}
func (d *LeaseRepoStub) DropLease(key, owner string) error {
	return d.err
}
func (d *LeaseRepoStub) RenewLease(key, owner string, counter int64) error {
	return d.err
}
func (d *LeaseRepoStub) UpdateCheckpoint(key, checkpoint, owner string, counter int64) error {
	return d.err
}
