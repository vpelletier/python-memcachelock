"""
Use memcached as a lock server.

API is a superset of thread.LockType: same methods, same positional and
named arguments, thread.error subclass exception raised when releasing
an unacquired lock.

RLock (and subclasses) is as thread-safe as provided memcache connection.
python-memcached is thread-safe (each thread has its own socket).

The goal of this class is not to provide a perfect networked lock.
Memcached prunes entries or looses them by restart, network may be
unreliable, etc. Each of these can lead to a lock being stolen.
The following constructor options affect lock theft detection/prevention:
- careful_release
  When True, lock theft is detected upon release (MemcacheLockReleaseError
  is raised). Note that theft may not be currently occuring, all this
  detects is that current instance does not own the lock - maybe none do.
  This has a (slight) performance cost.
- stand_in
  When True, operations succeed even when memcached is unreachable.
  Applications which check consistency after operation should enable this
  to prevent memcached unreachability from blocking them.

How it works:
- Locks protect access to a shared resource. The relationship between a
  resource and its lock is called a <key> in this class.
- Several lock instances can compete for a given resource. Each instance is
  identified by its <uid>, which can be auto-allocated.
- Upon acquisition of a (non-acquired) lock, "add" command is sent to
  memcached for <key> with lock's <uid> as value. This succeeds only if
  memcached didn't knew that <key>, and is atomic.
- Further acquisitions for re-entrant locks are locally handled, there is
  no network overhead.
- Upon release of an acquired lock, "delete" command is sent to memcached
  for <key>, releasing the lock. It is optionally (enabled by default)
  preceeded by a check that memcached still holds our <uid>, to avoid
  releasing some other instance's lock.

How to break things:
- create a lock instance, then 2**64 others (trash them along the way, you
  don't need to keep them). Next one will think it is the first instance.
- restart memcache server while one client has taken a lock (unles your
  memcache server restarts from persistent data properly written to disk).
- have memcache server prune entries
"""
import thread
import threading
import time
from functools import partial

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
    """
    Lock theft detected on release.
    """
    pass


class MemcacheLockUidError(MemcacheLockError):
    """
    UID allocation failed.
    See stand_in for a possible solution.
    """
    pass


class MemcacheLockNetworkError(MemcacheLockError):
    """
    Memcached could not be reached.
    See stand_in for a possible solution.
    """
    pass


class RLock(object):
    """
    Network-reentrant lock.

    Re-entry is per uid. An RLock instance can be simultaneously
    acquired by any number of threads.
    See ThreadRLock if you need thread-aware reentrant locking.
    """
    reentrant = True

    def __init__(
            self, client, key, interval=0.05, uid=None, exptime=0,
            persistent=True, careful_release=True, backoff=0, stand_in=False,
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
            Mandatory when stand_in is true.
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
        stand_in (bool)
            When memcached is unreachable and stand_in is true, pretend all
            operations succeed. This prevents application lockup when memcached
            goes away.
            You should consider disabling careful_release when enabling
            stand_in, otherwise you would get errors when memcached becomes
            available again.
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
            if stand_in:
                raise ValueError('uid must be provided when stand_in is true')
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
        self.stand_in = stand_in
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

    def _wait(self, timeout):
        """
        Timed generator, yielding with quadratic backoff and ending when
        timeout is exceeded.

        timeout (float)
            How long to keep generating, in seconds.
        """
        if timeout is None:
            timeout = INF
        deadline = time.time() + timeout
        interval = min(self.interval, timeout)
        while True:
            yield
            now = time.time()
            if now >= deadline:
                break
            time.sleep(min(interval, deadline - now))
            interval = min(self.backoff, interval * 2, deadline - time.time())

    def acquire(self, blocking=True, timeout=INF):
        """
        Acquire the lock.

        blocking (bool)
            When true, use given timeout.
            When false, set timeout to 0.
        timeout (float)
            How long to wait for lock, in seconds.
            float('inf') means block until lock can be acquired.
            For compatibility with standard multiprocessing module, None is
            treated as float('inf').

        Returns True if the lock could be acquired, False otherwise.

        If memcached cannot be reached and stand_in is disabled, raise
        MemcacheLockNetworkError. When stand_in is enabled, consider the
        acquirition as successful instead.
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
        tries = maxtries = 3
        for _ in self._wait(timeout if blocking else 0):
            if method(self.key, (self.uid, new_locked), self.exptime):
                break
            # python-memcached masquerades network errors as command failure,
            # so check if server is still alive by peeking at lock owner.
            if not self.memcache.get(self.key):
                if tries:
                    # Maybe lock was just released, retry immediately.
                    tries -= 1
                    continue
                if self.stand_in:
                    break
                raise MemcacheLockNetworkError
            tries = maxtries
            # I don't have the lock.
        else:
            return False
        self._locked = new_locked
        return True

    def release(self, timeout=0):
        """
        Release the lock.

        If lock is locally known as released, raise
        MemcacheLockReleaseError .
        If lock is discovered as not acquired on memcached:
        - if stand_in is disabled, raise MemcacheLockNetworkError
        - if stand_in is enabled, release the lock once
        If careful_release is enabled and lock theft is detected:
        - if stand_in is disabled, raise MemcacheLockReleaseError
        - if stand_in is enabled, release the lock once
        """
        if not self._locked:
            raise MemcacheLockReleaseError('release unlocked lock')
        if self.careful_release:
            owner = self.getOwnerUid()
            if owner != self.uid and (
                    owner is not None or not self.stand_in):
                raise MemcacheLockReleaseError(
                    '%s: should be owned by me (%s), but owned by %s' % (
                        self.key[1], self.uid, owner))
        self._locked -= 1
        if self._locked:
            if not self.persistent:
                return
            action = partial(
                self.memcache.replace,
                self.key, (self.uid, self._locked), self.exptime,
            )
        else:
            action = partial(
                self.memcache.delete,
                self.key,
            )
        for _ in self._wait(timeout):
            if action():
                break
        else:
            if not self.stand_in:
                raise MemcacheLockNetworkError

    def locked(self, by_self=False):
        """
        Tell if the lock is currently held.

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
    """
    Network non-reentrant lock.
    """
    reentrant = False


class ThreadRLock(RLock):
    """
    Thread-aware network reentrant lock.

    Combines a regular RLock with a network reentrant lock, so it can be used
    in a multithreaded process.
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
        """
        Acquire the lock.

        See memcachelock.RLock.acquire and thread.LockType.acquire .
        """
        if self._rlock.acquire(blocking):
            return super(ThreadRLock, self).acquire(blocking, **kw)
        return False

    def release(self, **kw):
        """
        Release the lock.

        See memcachelock.RLock.release and thread.LockType.release .
        """
        # This is sufficient, as:
        # - if we don't own rlock, it will raise (we won't release memcache)
        # - if memcache release raises, there is no way to recover (we thought
        #   we were owning the lock)
        self._rlock.release()
        super(ThreadRLock, self).release(**kw)

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
