import thread
import time

LOCK_UID_KEY_SUFFIX = '_uid'

class MemcacheLockError(Exception):
    pass

class MemcacheRLock(object):
    """
    Attempt at using memcached as a lock server, using gets/cas command pair.
    Inspired by unimr.memcachedlock .
    There is no queue management: lock wil not be granted in the order it was
    requested, but to the process which reaches memcache server at the right
    time.
    See thread.LockType documentation for API.

    How to break things:
    - create a lock instance, then 2**32 others (trash them along the way, you
      don't need to keep them). Next one will think it is the first instance.
    - restart memcache server while one client has taken a lock (unles your
      memcache server restarts from persistent data properly written to disk).
    - have memcache server prune entries
    """
    reentrant = True

    def __init__(self, client, key, interval=0.05):
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
        if client.gets(uid_key) is None:
            # Nobody has used this lock yet (or it was lost in a server
            # restart). Init to 0. Don't care if it fails, we just need a
            # value to be set.
            client.cas(uid_key, 0)
        self.uid = client.incr(uid_key)
        assert self.uid is not None
        self.interval = interval

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
            raise MemcacheLockError('Lock stolen')

class MemcacheLock(MemcacheRLock):
    reentrant = False

