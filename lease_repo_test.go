package kinesis

type LeaseRepoMock struct {
	*LeaseRepoStub
	createLeaseCalls []*lease
	renewLeaseCalls  []struct {
		key     string
		owner   string
		counter int64
	}
}

func NewLeaseRepoMock() *LeaseRepoMock {
	return &LeaseRepoMock{
		LeaseRepoStub: &LeaseRepoStub{},
	}
}

func (d *LeaseRepoMock) CreateLeaseIfNotExists(l *lease) error {
	d.createLeaseCalls = append(d.createLeaseCalls, l)
	return d.err
}

func (d *LeaseRepoMock) RenewLease(key, owner string, counter int64) error {
	d.renewLeaseCalls = append(d.renewLeaseCalls, struct {
		key     string
		owner   string
		counter int64
	}{key, owner, counter})

	return d.err
}

type LeaseRepoStub struct {
	leases []*lease
	err    error
}

func (d *LeaseRepoStub) GetLeases() ([]*lease, error) {
	return d.leases, d.err
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
