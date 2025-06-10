package lock

import (
	"encoding/json"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

const (
	LOCK_FREE   = "FREE"
	LOCK_LOCKED = "LOCKED"
)

type LockState struct {
	ID    string
	State string
}

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	ID       string
	Key      string
	PutMaybe bool
	Version  rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.Key = l
	lk.ID = kvtest.RandValue(8)
	lk.PutMaybe = false
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	// Spinlock
spin:
	for {
		var state LockState
		stateStr, version, err := lk.ck.Get(lk.Key)

		json.Unmarshal([]byte(stateStr), &state)

		switch err {
		case rpc.ErrNoKey:
			// First acquire of lock
			state.ID = lk.ID
			state.State = LOCK_LOCKED
			stateBytes, _ := json.Marshal(state)
			err = lk.ck.Put(lk.Key, string(stateBytes), rpc.Tversion(0))
			if err == rpc.OK {
				// Success
				lk.Version = rpc.Tversion(1)
				break spin
			} else if err == rpc.ErrMaybe {
				// The lock request may be processed by the server
				// Check on next loop.
				lk.PutMaybe = true
			}
			// Another node acquired the lock before us
			// Retry acquire
			break
		case rpc.OK:
			if state.State == LOCK_LOCKED {
				if lk.PutMaybe {
					lk.PutMaybe = false
					if state.ID == lk.ID {
						// Confirmed lock success, break
						break spin
					}
				}
				// Retry acquire
				break
			}
			state.ID = lk.ID
			state.State = LOCK_LOCKED
			stateBytes, _ := json.Marshal(state)
			err = lk.ck.Put(lk.Key, string(stateBytes), version)
			if err == rpc.OK {
				// Success
				lk.Version = version + 1
				break spin
			} else if err == rpc.ErrMaybe {
				// The lock request may be processed by the server
				// Check on next loop.
				lk.PutMaybe = true
			}
			// Another node acquired the lock before us
			// Retry acquire
			break
		}

		time.Sleep(10 * time.Millisecond) // 10ms cooldown
	}
}

func (lk *Lock) Release() {
	// Your code here
spin:
	for {
		state := LockState{
			ID:    lk.ID,
			State: LOCK_FREE,
		}
		stateBytes, _ := json.Marshal(state)
		err := lk.ck.Put(lk.Key, string(stateBytes), lk.Version)
		switch err {
		case rpc.OK:
			// Release success
			break spin
		default: // Other error...
			// Try to acquire a new version since it's somehow modified...
			stateStr, version, err := lk.ck.Get(lk.Key)
			json.Unmarshal([]byte(stateStr), &state)
			switch err {
			case rpc.OK:
				if state.State == LOCK_FREE {
					// Lock is already released
					break spin
				} else if state.ID != lk.ID {
					// Lock ownership got transferred to others
					// This can happen when Put LOCK_FREE returns ErrMaybe
					// Giveup release
					break spin
				}
				// Lock is still locked, update version & retry
				lk.Version = version
				break
			case rpc.ErrNoKey:
				// Lock somehow got removed, insert lock LOCK_FREE
				lk.Version = rpc.Tversion(0)
				break
			}
		}

		time.Sleep(10 * time.Millisecond) // 10ms cooldown
	}
}
