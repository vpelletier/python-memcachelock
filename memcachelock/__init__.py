import thread
import threading
import time

LOCK_UID_KEY_SUFFIX = '_uid'

class MemcacheLockError(Exception):
    """
    Unexpected memcached reaction.
    Either memcached is misbehaving (refusing to hold a value...) or a
    competitor could acquire the lock (because of software error, or memcached
    evicted data we needed...).
    """
    pass

class MemcacheLockCasError(MemcacheLockError):
    pass

class MemcacheLockGetsError(MemcacheLockError):
    pass

class MemcacheLockReleaseError(MemcacheLockError, thread.error):
    def __init__(self, key, releasing_uid, owner_uid):
        super(MemcacheLockReleaseError, self).__init__()
        self.key = key
        self.releasing_uid = releasing_uid
        self.owner_uid = owner_uid

    def __str__(self):
        return '%s: should be owned by me (%s), but owned by %s' % (
            self.key,
            self.releasing_uid,
            self.owner_uid,
        )

class MemcacheLockUidError(MemcacheLockError):
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

    def __init__(self, client, key, interval=0.05, uid=None, exptime=0):
        """
        client (memcache.Client)
            Memcache connection. Must support cas.
            (by default, python-memcached DOES NOT unless you set cache_cas)
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
        exptime (int)
            memcache-style expiration time. See memcache protocol
            documentation on <exptime>.
        """
        if getattr(client, 'cas', None) is None or getattr(client, 'gets',
                None) is None:
            raise TypeError('Client does not implement "gets" and/or "cas" '
                'methods.')
        if not getattr(client, 'cache_cas', True):
            raise TypeError('Client cache_cas is disabled, "cas" will not '
                'work.')
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
            if uid is None:
                raise MemcacheLockUidError('incr failed to give number')
        self.uid = uid
        self.interval = interval
        self.exptime = exptime

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
                except MemcacheLockCasError:
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
            raise MemcacheLockReleaseError(self.key[1], self.uid, owner)
        assert count > 0
        self.__set(count - 1)

    def locked(self, by_self=False):
        """
        by_self (bool, False)
            If True, returns whether this instance holds this lock.
        """
        owner_uid = self.__get()[0]
        return by_self and owner_uid == self.uid or owner_uid is not None

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
                raise MemcacheLockGetsError('Memcache not storing anything')
        return value

    def __set(self, count):
        if not self.memcache.cas(self.key, (count and self.uid or None, count),
                self.exptime):
            raise MemcacheLockCasError('Lock stolen')

class Lock(RLock):
    reentrant = False

class ThreadRLock(object):
    """
    Thread-aware RLock.

    Combines a regular RLock with a RLock, so it can be used in a
    multithreaded app like an RLock, in addition to RLock behaviour.
    """
    def __init__(self, *args, **kw):
        # Local RLock-ing
        self._rlock = threading.RLock()
        # Remote RLock-ing
        self._memcachelock = RLock(*args, **kw)

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

# BBB
MemcacheLock = Lock
MemcacheRLock = RLock
ThreadMemcacheRLock = ThreadRLock
