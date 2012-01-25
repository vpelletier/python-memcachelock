import thread
import threading
import time

LOCK_UID_KEY_SUFFIX = '_uid'

class MemcacheLockError(Exception):
    """
    Lock acquired by another instance.
    If caught outside MemcacheRLock/MemcacheLock instance, means a competitor
    stole the lock or something is racing against us.
    """
    pass

class MemcacheRLock(object):
    """
    Attempt at using memcached as a lock server, using gets/cas command pair.
    Inspired by unimr.memcachedlock .
    There is no queue management: lock will not be granted in the order it was
    requested, but to the requester reaching memcache server at the right time.
    See thread.LockType documentation for API.

    Note: MemcacheRLock ignores local threads. So it is not a drop-in
    replacement for python's RLock. See class ThreadMemcacheRLock.

    How to break things:
    - create a lock instance, then 2**64 others (trash them along the way, you
      don't need to keep them). Next one will think it is the first instance.
    - restart memcache server while one client has taken a lock (unles your
      memcache server restarts from persistent data properly written to disk).
    - have memcache server prune entries
    """
    reentrant = True

    def __init__(self, client, key, interval=0.05, uid=None):
        """
        client (memcache.Client)
            Memcache connection.
        key (str)
            Unique identifier for protected resource, common to all locks
            protecting this resource.
            Can be any reasonable string for use as a memcache key.
            Must not end with LOCK_UID_KEY_SUFFIX.
        interval (int/float, 0.05)
            Period between consecutive lock taking attemps in blocking mode.
        uid (any picklable object, None)
            Unique lock instance identifier for given key.
            If None, a new uid will be generated for each new instance.
            Allows overriding default uid allocation. Can also be handy to
            recover lock when previous instance with given uid died with lock
            acquired.
            WARNING: You must be very sure of what you do before fiddling with
            this parameter. Especially, don't mix up auto-allocated uid and
            provided uid on the same key. You have been warned.
        """
        if getattr(client, 'cas', None) is None or getattr(client, 'gets',
                None) is None:
            raise TypeError('Client does not implement "gets" and/or "cas" '
                'methods.')
        if key.endswith(LOCK_UID_KEY_SUFFIX):
            raise ValueError('Key conflicts with internal lock storage key '
                '(ends with ' + LOCK_UID_KEY_SUFFIX + ')')
        self.memcache = client
        # Compute hash once only. Also used to keep lock uid close to the
        # value it manages.
        key_hash = hash(key)
        self.key = (key_hash, key)
        uid_key = (key_hash, key + LOCK_UID_KEY_SUFFIX)
        client.check_key(uid_key[1])
        if uid is None:
            if client.gets(uid_key) is None:
                # Nobody has used this lock yet (or it was lost in a server
                # restart). Init to 0. Don't care if it fails, we just need a
                # value to be set.
                client.cas(uid_key, 0)
            uid = client.incr(uid_key)
        self.uid = uid
        self.interval = interval

    def __repr__(self):
        return '<%s(key=%r, interval=%r, uid=%r) at 0x%x>' % (
            self.__class__.__name__,
            self.key[1],
            self.interval,
            self.uid,
            id(self),
        )

    def acquire(self, blocking=True):
        while True:
            owner, count = self.__get()
            if owner == self.uid:
                # I have the lock already.
                assert count
                if self.reentrant:
                    self.__set(count + 1)
                    return True
            elif owner is None:
                # Nobody had it on __get call, try to acquire it.
                try:
                    self.__set(1)
                except MemcacheLockError:
                    # Someting else was faster.
                    pass
                else:
                    # I got the lock.
                    return True
            # I don't have the lock.
            if not blocking:
                break
            time.sleep(self.interval)
        return False

    def release(self):
        owner, count = self.__get()
        if owner != self.uid:
            raise thread.error('release unlocked lock')
        assert count > 0
        self.__set(count - 1)

    def locked(self):
        return self.__get()[0] is not None

    def getOwnerUid(self):
        """
        Return lock owner's uid.
        Purely informative. Chances are this will not be true anymore by the
        time caller gets this value. Can be handy to recover a lock (see
        constructor's "uid" parameter - and associated warning).
        """
        return self.__get()[0]

    __enter__ = acquire

    def __exit__(self, t, v, tb):
        self.release()

    # BBB
    acquire_lock = acquire
    locked_lock = locked
    release_lock = release

    def __get(self):
        value = self.memcache.gets(self.key)
        if value is None:
            # We don't care if this call fails, we just want to initialise
            # the value.
            self.memcache.add(self.key, (None, 0))
            value = self.memcache.gets(self.key)
            if value is None:
                raise MemcacheLockError('Memcached caught fire')
        return value

    def __set(self, count):
        if not self.memcache.cas(self.key, (count and self.uid or None, count)):
            raise MemcacheLockError('Lock stolen')

class MemcacheLock(MemcacheRLock):
    reentrant = False

class ThreadMemcacheRLock(object):
    """
    Thread-aware MemcacheRLock.

    Combines a regular RLock with a MemcacheRLock, so it can be used in a
    multithreaded app like an RLock, in addition to MemcacheRLock behaviour.
    """
    def __init__(self, *args, **kw):
        # Local RLock-ing
        self._rlock = threading.RLock()
        # Remote RLock-ing
        self._memcachelock = MemcacheRLock(*args, **kw)

    def acquire(self, blocking=True):
        if self._rlock.acquire(blocking):
            return self._memcachelock.acquire(blocking)
        return False

    def release(self):
        # This is sufficient, as:
        # - if we don't own rlock, it will raise (we won't release memcache)
        # - if memcache release raises, there is no way to recover (we thought
        #   we were owning the lock)
        self._rlock.release()
        self._memcachelock.release()

    __enter__ = acquire

    def __exit__(self, t, v, tb):
        self.release()

    def locked(self):
        if self._rlock.acquire(False):
            try:
                return self._memcachelock.locked()
            finally:
                self._rlock.release()
        return False

    @property
    def uid(self):
        return self._memcachelock.uid

    def getOwnerUid(self):
        return self._memcachelock.getOwnerUid()

    # BBB
    acquire_lock = acquire
    locked_lock = locked
    release_lock = release

if __name__ == '__main__':
    # Run simple tests.
    # Only verifies the API, no race test is done.
    import memcache
    HOSTS = ['127.0.0.1:11211']
    TEST_KEY_1 = 'foo'
    TEST_KEY_2 = 'bar'
    mc1 = memcache.Client(HOSTS)
    mc2 = memcache.Client(HOSTS)
    mc3 = memcache.Client(HOSTS)
    mc1.delete_multi((TEST_KEY_1, TEST_KEY_2, TEST_KEY_1 + LOCK_UID_KEY_SUFFIX,
        TEST_KEY_2 + LOCK_UID_KEY_SUFFIX))

    for klass in (MemcacheLock, MemcacheRLock, ThreadMemcacheRLock):
        # Two locks sharing the same key, a third for crosstalk checking.
        locka1 = klass(mc1, TEST_KEY_1)
        locka2 = klass(mc2, TEST_KEY_1)
        lockb1 = klass(mc3, TEST_KEY_2)
        print klass, locka1.uid, locka2.uid, lockb1.uid

        def checkLocked(a1=False, a2=False, b1=False):
            assert locka1.locked() == a1
            assert locka2.locked() == a2
            assert lockb1.locked() == b1

        checkLocked()
        assert locka1.acquire(False)
        checkLocked(a1=True, a2=True)
        if klass is MemcacheLock:
            assert not locka1.acquire(False)
        assert not locka2.acquire(False)
        checkLocked(a1=True, a2=True)
        try:
            locka2.release()
        except thread.error:
            pass
        else:
            raise AssertionError('Should have raised')
        checkLocked(a1=True, a2=True)
        assert locka2.getOwnerUid() == locka1.uid
        locka1.release()
        checkLocked()
        assert locka1.acquire()
        del locka1
        # Lock still held, although owner instance died
        assert locka2.locked()
        del locka2
        del lockb1
        mc1.delete_multi((TEST_KEY_1, TEST_KEY_2))

    for klass in (MemcacheRLock, ThreadMemcacheRLock):
        lock = klass(mc1, TEST_KEY_1)
        assert lock.acquire(False)
        assert lock.acquire(False)
        lock.release()
        assert lock.locked()
        lock.release()
        assert not lock.locked()

    # I just need a mutable object. Event happens to have the API I need.
    success = threading.Event()
    def release(lock):
        try:
            lock.release()
        except (RuntimeError, thread.error):
            pass
        else:
            success.set()
    lock = MemcacheRLock(mc1, TEST_KEY_1)
    lock.acquire()
    release_thread = threading.Thread(target=release, args=(lock, ))
    release_thread.daemon = True
    release_thread.start()
    release_thread.join(1)
    assert not release_thread.is_alive()
    assert not lock.locked()
    assert success.is_set()
    success.clear()
    lock = ThreadMemcacheRLock(mc1, TEST_KEY_1)
    lock.acquire()
    release_thread = threading.Thread(target=release, args=(lock, ))
    release_thread.daemon = True
    release_thread.start()
    release_thread.join(1)
    assert not release_thread.is_alive()
    assert lock.locked()
    assert not success.is_set()
    lock.release()

    print 'Passed.'

