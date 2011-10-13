import thread
import time

LOCK_UID_KEY_SUFFIX = '_uid'

class MemcacheLockError(Exception):
    pass

class MemcacheRLock(object):
    """
    Attempt at using memcached as a lock server, using gets/cas command pair.
    Inspired by unimr.memcachedlock .
    There is no queue management: lock will not be granted in the order it was
    requested, but to the requester reaching memcache server at the right time.
    See thread.LockType documentation for API.

    How to break things:
    - create a lock instance, then 2**32 others (trash them along the way, you
      don't need to keep them). Next one will think it is the first instance.
    - restart memcache server while one client has taken a lock (unles your
      memcache server restarts from persistent data properly written to disk).
    - have memcache server prune entries
    """
    reentrant = True
    # Keep a reference to MemcacheLockError so we can rely on its presence
    # when __del__ gets called during interpreter shutdown.
    MemcacheLockError = MemcacheLockError

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
                except self.MemcacheLockError:
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
    __exit__ = release
    # BBB
    acquire_lock = acquire
    locked_lock = locked
    release_lock = release

    def __get(self):
        value = self.memcache.gets(self.key)
        if value is None:
            value = (None, 0)
        return value

    def __set(self, count):
        if not self.memcache.cas(self.key, (count and self.uid or None, count)):
            raise self.MemcacheLockError('Lock stolen')

    def __del__(self):
        owner, _ = self.__get()
        if owner == self.uid:
            try:
                self.__set(0)
            except self.MemcacheLockError:
                pass

class MemcacheLock(MemcacheRLock):
    reentrant = False

