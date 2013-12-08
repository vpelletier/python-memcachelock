import thread
import threading
import time

LOCK_UID_KEY_SUFFIX = '_uid'
INF = float('inf')


class MemcacheLockError(Exception):
    """
    Unexpected memcached reaction.
    Either memcached is misbehaving (refusing to hold a value...) or a
    competitor could acquire the lock (because of software error, or memcached
    evicted data we needed...).
    """
    pass


class MemcacheLockReleaseError(MemcacheLockError, thread.error):
    pass


class MemcacheLockUidError(MemcacheLockError):
    pass


class MemcacheLockNetworkError(MemcacheLockError):
    """
    Memcached could not be reached.
    See stand_in for a possible solution.
    """
    pass


class RLock(object):
    """
    Attempt at using memcached as a lock server, using gets/cas command pair.
    Inspired by unimr.memcachedlock .
    There is no queue management: lock will not be granted in the order it was
    requested, but to the requester reaching memcache server at the right time.
    See thread.LockType documentation for API.

    Note: RLock ignores local threads. So it is not a drop-in replacement for
    python's RLock. See class ThreadRLock.

    How to break things:
    - create a lock instance, then 2**64 others (trash them along the way, you
      don't need to keep them). Next one will think it is the first instance.
    - restart memcache server while one client has taken a lock (unles your
      memcache server restarts from persistent data properly written to disk).
    - have memcache server prune entries
    """
    reentrant = True

    def __init__(
            self, client, key, interval=0.05, uid=None, exptime=0,
            persistent=True, careful_release=True, backoff=0,
            ):
        """
        client (memcache.Client)
            Memcache connection.
        key (str)
            Unique identifier for protected resource, common to all locks
            protecting this resource.
            Can be any reasonable string for use as a memcache key.
            Must not end with LOCK_UID_KEY_SUFFIX.
        interval (float)
            Period between consecutive lock taking attemps in blocking mode.
        uid (any picklable object)
            Unique lock instance identifier for given key.
            If None, a new uid will be generated for each new instance.
            Allows overriding default uid allocation. Can also be handy to
            recover lock when previous instance with given uid died with lock
            acquired.
            WARNING: You must be very sure of what you do before fiddling with
            this parameter. Especially, don't mix up auto-allocated uid and
            provided uid on the same key. You have been warned.
        exptime (int)
            memcache-style expiration time. See memcache protocol
            documentation on <exptime>.
        persistent (bool)
            Whether to restore previous lock acquisition state on a new
            instance.
            Costs one network roundtrip on each reentrant action (non-first
            acquisition and non-last release).
            When exptime is an offset, reentrant actions extend lock lifespan.
        careful_release (bool)
            Get lock owner from memcached before releasing lock, to detect
            theft. Disabling this test saves a network roundtrip for each
            release.
        backoff (float)
            Upper bound of quadratic <interval> backoff.
            Quadratic backoff is disabled when less or equal to <interval>.
        """
        if key.endswith(LOCK_UID_KEY_SUFFIX):
            raise ValueError(
                'Key conflicts with internal lock storage key (ends with ' +
                LOCK_UID_KEY_SUFFIX + ')')
        self.memcache = client
        # Compute hash once only. Also used to keep lock uid close to the
        # value it manages.
        key_hash = hash(key)
        self.key = (key_hash, key)
        uid_key = (key_hash, key + LOCK_UID_KEY_SUFFIX)
        client.check_key(uid_key[1])
        if uid is None:
            client.add(uid_key, 0)
            uid = client.incr(uid_key)
            if uid is None:
                raise MemcacheLockUidError('incr failed to give number')
        self.uid = uid
        self.interval = interval
        self.exptime = exptime
        self.persistent = persistent
        self.careful_release = careful_release
        self.backoff = max(interval, backoff)
        self.resync()

    def resync(self):
        """
        Resynchronise internal state with memcached state.

        Cures stuck lock object when memcached evicted an entry (especially
        when exptime is used).
        """
        owner, locked = self.__get()
        if owner != self.uid:
            locked = 0
        self._locked = locked

    def __repr__(self):
        return '<%s(key=%r, interval=%r, uid=%r) at 0x%x>' % (
            self.__class__.__name__,
            self.key[1],
            self.interval,
            self.uid,
            id(self),
        )

    def acquire(self, blocking=True, timeout=None):
        """
        timeout (float, None)
            How long to wait for lock, in seconds.
        """
        if self._locked and self.reentrant:
            new_locked = self._locked + 1
            if self.persistent:
                method = self.memcache.replace
            else:
                self._locked = new_locked
                return True
        else:
            new_locked = 1
            method = self.memcache.add
        if timeout is None:
            deadline = INF
            interval = self.interval
        else:
            deadline = time.time() + timeout
            interval = min(self.interval, timeout)
        retrying = False
        while True:
            if method(self.key, (self.uid, new_locked), self.exptime):
                break
            # python-memcached masquerades network errors as command failure,
            # so check if server is still alive by peeking at lock owner.
            if not self.memcache.get(self.key):
                if retrying:
                    raise MemcacheLockNetworkError
                # Maybe lock was just released, retry immediately.
                retrying = True
                continue
            retrying = False
            # I don't have the lock.
            if not blocking or time.time() >= deadline:
                return False
            time.sleep(interval)
            interval = min(self.backoff, interval * 2, deadline - time.time())
        self._locked = new_locked
        return True

    def release(self):
        if not self._locked:
            raise MemcacheLockReleaseError('release unlocked lock')
        if self.careful_release:
            owner = self.getOwnerUid()
            if owner != self.uid:
                raise MemcacheLockReleaseError(
                    '%s: should be owned by me (%s), but owned by %s' % (
                        self.key[1], self.uid, owner))
        self._locked -= 1
        if self._locked:
            if self.persistent and not self.memcache.replace(
                    self.key, (self.uid, self._locked), self.exptime,
                    ):
                raise MemcacheLockNetworkError
        elif not self.memcache.delete(self.key):
            raise MemcacheLockNetworkError

    def locked(self, by_self=False):
        """
        by_self (bool)
            If True, returns whether this instance holds this lock.
        """
        if by_self:
            return bool(self._locked)
        return bool(self.getOwnerUid())

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

    def __del__(self):
        if not self.persistent:
            while self._locked:
                self.release()

    # BBB
    acquire_lock = acquire
    locked_lock = locked
    release_lock = release

    def __get(self):
        value = self.memcache.get(self.key)
        if value is None:
            return None, 0
        return value


class Lock(RLock):
    reentrant = False


class ThreadRLock(RLock):
    """
    Thread-aware RLock.

    Combines a regular RLock with a RLock, so it can be used in a
    multithreaded app like an RLock, in addition to RLock behaviour.
    """
    def __init__(self, *args, **kw):
        """
        See memcachelock.RLock.__init__ .

        persistence is not available, as it is not possible to restore
        threading.RLock's state.
        """
        # Local RLock-ing
        self._rlock = threading.RLock()
        super(ThreadRLock, self).__init__(persistent=False, *args, **kw)

    def acquire(self, blocking=True, **kw):
        if self._rlock.acquire(blocking):
            return super(ThreadRLock, self).acquire(blocking, **kw)
        return False

    def release(self):
        # This is sufficient, as:
        # - if we don't own rlock, it will raise (we won't release memcache)
        # - if memcache release raises, there is no way to recover (we thought
        #   we were owning the lock)
        self._rlock.release()
        super(ThreadRLock, self).release()

    __enter__ = acquire

    def __exit__(self, t, v, tb):
        self.release()

    def locked(self, by_self=False):
        """
        Tell if the lock is currently held.

        See memcachelock.RLock.locked .
        """
        # by_self | has_lock | return
        # False   | False    | memcache
        # False   | True     | memcache
        # True    | False    | False
        # True    | True     | memcache
        has_lock = self._rlock.acquire(False)
        if by_self and not has_lock:
            return False
        try:
            return super(ThreadRLock, self).locked(by_self=by_self)
        finally:
            if has_lock:
                self._rlock.release()

    def __del__(self):
        # Do not release self._RLock, since we can delete instance from a
        # thread which didn't own the lock.
        release = super(ThreadRLock, self).release
        while self._locked:
            release()

    # BBB
    acquire_lock = acquire
    locked_lock = locked
    release_lock = release

# BBB
MemcacheLock = Lock
MemcacheRLock = RLock
ThreadMemcacheRLock = ThreadRLock
