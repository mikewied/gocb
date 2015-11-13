package gocb

import (
	"sync"
	"time"

	"github.com/couchbase/gocb/gocbcore"
)

type bucketInternal struct {
	b *Bucket;
}

// Retrieves a document from the bucket
func (bi *bucketInternal) GetRandom(valuePtr interface{}) (string, Cas, error) {
	return bi.b.getRandom(valuePtr)
}

// Inserts or replaces (with meta) a document in a bucket.
func (bi *bucketInternal) UpsertMeta(key string, value, extra []byte, flags, expiry uint32, cas, revseqno uint64) (Cas, error) {
	outcas, _, err := bi.b.upsertMeta(key, value, extra, flags, expiry, cas, revseqno)
	return outcas, err
}

// Removes a document (with meta) from the bucket.
func (bi *bucketInternal) RemoveMeta(key string, extra []byte, flags, expiry uint32, cas, revseqno uint64) (Cas, error) {
	outcas, _, err := bi.b.removeMeta(key, extra, flags, expiry, cas, revseqno)
	return outcas, err
}

func (b *Bucket) getRandom(valuePtr interface{}) (keyOut string, casOut Cas, errOut error) {
	signal := make(chan bool, 1)
	op, err := b.client.GetRandom(func(keyBytes, bytes []byte, flags uint32, cas gocbcore.Cas, err error) {
		errOut = err
		if errOut == nil {
			errOut = b.transcoder.Decode(bytes, flags, valuePtr)
			if errOut == nil {
				casOut = Cas(cas)
				keyOut = string(keyBytes)
			}
		}
		signal <- true
	})
	if err != nil {
		return "", 0, err
	}

	timeoutTmr := gocbcore.AcquireTimer(b.opTimeout)
	select {
	case <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		op.Cancel()
		return "", 0, timeoutError{}
	}
}

func (b *Bucket) upsertMeta(key string, value, extra []byte, flags uint32, expiry uint32, cas, revseqno uint64) (Cas, MutationToken, error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.SetMeta([]byte(key), value, extra, flags, expiry, cas, revseqno, gocbcore.StoreCallback(cb))
		return op, err
	})
}

func (b *Bucket) removeMeta(key string, extra []byte, flags uint32, expiry uint32, cas, revseqno uint64) (Cas, MutationToken, error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.DeleteMeta([]byte(key), extra, flags, expiry, cas, revseqno, gocbcore.RemoveCallback(cb))
		return op, err
	})
}

type FutureStatus int

const (
	Pending   = FutureStatus(1)
	Done      = FutureStatus(2)
	Cancelled = FutureStatus(3)
	Timeout   = FutureStatus(4)
)

type Future struct {
	op     gocbcore.PendingOp
	cas    Cas
	err    error
	status FutureStatus
	wg     sync.WaitGroup
	mutex  sync.Mutex
}

func (f *Future) initialize(count int) {
	f.status = Pending
	f.wg.Add(count)
}

func (f *Future) setOperation(op gocbcore.PendingOp) {
	f.op = op
}

func (f *Future) complete(cas Cas, err error) {
	f.cas = cas
	f.err = err
	f.mutex.Lock()
	if f.status == Pending {
		f.wg.Done()
		f.status = Done
	}
	f.mutex.Unlock()
}

func (f *Future) Get() (Cas, error) {
	t := time.AfterFunc(5 * time.Second, func() {
		f.mutex.Lock()
		if f.status == Pending {
			f.wg.Done()
			f.status = Timeout
		}
		f.mutex.Unlock()
	})

	f.wg.Wait()
	t.Stop()
	return f.cas, f.err
}

func (f *Future) Cancel() {
	f.mutex.Lock()
	if f.status == Pending {
		f.wg.Done()
		f.op.Cancel()
		f.status = Done
	}
	f.mutex.Unlock()
}

func (bi *bucketInternal) AsyncUpsertMeta(key string, value, extra []byte, flags uint32, expiry uint32, cas, revseqno uint64) *Future {
	rv := &Future{}
	rv.initialize(1)
	op, err := bi.b.client.SetMeta([]byte(key), value, extra, flags, expiry, cas, revseqno,
		func(cas gocbcore.Cas, mt gocbcore.MutationToken, err error) {
			rv.complete(Cas(cas), err)
	})

	rv.setOperation(op)

	if err != nil {
		rv.complete(Cas(0), err)
	}

	return rv
}

func (bi *bucketInternal) AsyncRemoveMeta(key string, extra []byte, flags uint32, expiry uint32, cas, revseqno uint64) *Future {
	rv := &Future{}
	rv.initialize(1)
	op, err := bi.b.client.DeleteMeta([]byte(key), extra, flags, expiry, cas, revseqno,
		func(cas gocbcore.Cas, mt gocbcore.MutationToken, err error) {
			rv.complete(Cas(cas), err)
	})

	rv.setOperation(op)

	if err != nil {
		rv.complete(Cas(0), err)
	}

	return rv
}
