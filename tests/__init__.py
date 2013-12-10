"""These tests rely on an accessible memcache server"""

import thread
import threading
import time
import multiprocessing
import gc
from functools import partial

import memcache
from memcachelock import RLock, Lock, ThreadRLock, LOCK_UID_KEY_SUFFIX, \
    MemcacheLockReleaseError

import unittest


TEST_HOSTS = ['127.0.0.1:11211']
TEST_KEY_1 = 'foo'
TEST_KEY_2 = 'bar'


def _delete_test_keys():
    """Make sure no old keys are lying around"""
    memcache.Client(TEST_HOSTS).delete_multi(
        (TEST_KEY_1, TEST_KEY_2, TEST_KEY_1 + LOCK_UID_KEY_SUFFIX,
         TEST_KEY_2 + LOCK_UID_KEY_SUFFIX)
    )


def setUpModule():
    _delete_test_keys()


def tearDownModule():
    _delete_test_keys()


class TestLock(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mc1 = memcache.Client(TEST_HOSTS, cache_cas=True)
        cls.mc2 = memcache.Client(TEST_HOSTS, cache_cas=True)
        cls.mc3 = memcache.Client(TEST_HOSTS, cache_cas=True)

    def tearDown(self):
        gc.collect()
        self.mc1.delete_multi((TEST_KEY_1, TEST_KEY_2))

    def check(self, locked, unlocked):
        for lock in locked:
            self.assertTrue(lock.locked())
        for lock in unlocked:
            self.assertFalse(lock.locked())


class TestBasicLogic(TestLock):
    """Run simple tests.

    Only verifies the API, no race test is done.
    """
    def _test_normal(self, LockType):
        # Two locks sharing the same key, a third for crosstalk checking.
        locka1 = LockType(self.mc1, TEST_KEY_1)
        locka2 = LockType(self.mc2, TEST_KEY_1)
        lockb1 = LockType(self.mc3, TEST_KEY_2)

        self.check([], unlocked=[locka1, locka2, lockb1])

        self.assertTrue(locka1.acquire(False))
        self.check([locka1, locka2], unlocked=[lockb1])

        if LockType is Lock:
            self.assertFalse(locka1.acquire(False))

        self.assertFalse(locka2.acquire(False))
        self.check([locka1, locka2], unlocked=[lockb1])

        self.assertRaises(thread.error, locka2.release)
        self.check([locka1, locka2], unlocked=[lockb1])

        self.assertEquals(locka2.getOwnerUid(), locka1.uid)

        locka1.release()
        self.check([], unlocked=[locka1, locka2, lockb1])

        self.assertTrue(locka1.acquire())

        if LockType is not ThreadRLock:
            del locka1
            # Lock still held, although owner instance died
            self.assertTrue(locka2.locked())

    def _test_reentrant(self, LockType):
        # Basic RLock-ish behaviour
        lock = LockType(self.mc1, TEST_KEY_1)
        self.assertTrue(lock.acquire(False))
        self.assertTrue(lock.acquire(False))
        lock.release()
        self.assertTrue(lock.locked())
        lock.release()
        self.assertFalse(lock.locked())

    def _test_reentrant_thread(self, LockType):
        """Return whether the lock was acquired inside the thread"""
        # I just need a mutable object. Event happens to have the API I need.
        success = threading.Event()

        def release(lock):
            if lock.acquire(False):
                success.set()

        lock = LockType(self.mc1, TEST_KEY_1)
        lock.acquire()
        release_thread = threading.Thread(target=release, args=(lock, ))
        release_thread.daemon = True
        release_thread.start()
        release_thread.join(1)
        self.assertFalse(release_thread.is_alive())
        return success.is_set()

    def _test_persistent(self, LockType):
        UID = 1
        if LockType is ThreadRLock:
            # Persistence is not supported, and neither is "persistent"
            # argument.
            lock1 = LockType(self.mc1, TEST_KEY_1, uid=UID)
            lock2 = LockType(self.mc1, TEST_KEY_1, uid=UID + 1)
        else:
            lock = LockType(self.mc1, TEST_KEY_1, uid=UID)
            self.assertTrue(lock.acquire(False))
            before_destruction = self.mc1.get(TEST_KEY_1)
            # Lock is not released on destruction
            del lock
            gc.collect()
            self.assertEqual(self.mc1.get(TEST_KEY_1), before_destruction)
            # Lock state is initialised from memcached on creation
            lock = LockType(self.mc1, TEST_KEY_1, uid=UID)
            self.assertTrue(lock.locked(by_self=True))
            lock1 = LockType(self.mc1, TEST_KEY_1, uid=UID, persistent=False)
            # Lock state is still initialised from memcached on creation
            self.assertTrue(lock1.locked(by_self=True))
            # ...so release it before creating lock2
            lock1.release()
            lock2 = LockType(
                self.mc1, TEST_KEY_1, uid=UID + 1, persistent=False)
        # When not persistent, lock is released on owner destruction
        self.assertTrue(lock1.acquire(False))
        if LockType is not Lock:
            # Acquire once more to exercice unlock loop on destruction
            self.assertTrue(lock1.acquire(False))
        before_destruction = self.mc1.get(TEST_KEY_1)
        # lock2 does not own the lock, so it releases nothing
        del lock2
        gc.collect()
        self.assertEqual(before_destruction, self.mc1.get(TEST_KEY_1))
        # ...but lock1 does, and releases
        del lock1
        gc.collect()
        self.assertEqual(None, self.mc1.get(TEST_KEY_1))

    def _test_careful_release(self, LockType):
        thief = 'lock_thief'
        # With careful_release enabled, stolen lock is detected
        lock1 = LockType(self.mc1, TEST_KEY_1)
        self.assertTrue(lock1.acquire(False))
        before_theft = self.mc1.get(TEST_KEY_1)
        self.mc1.set(TEST_KEY_1, (thief, 1))
        self.assertRaises(MemcacheLockReleaseError, lock1.release)
        # ...and lock is still granted to thief
        self.assertEqual(lock1.getOwnerUid(), thief)
        self.mc1.set(TEST_KEY_1, before_theft)
        # Without, it's not
        lock2 = LockType(self.mc1, TEST_KEY_2, careful_release=False)
        self.assertTrue(lock2.acquire(False))
        self.mc1.set(TEST_KEY_2, thief)
        lock2.release()
        # ...and lock is really released
        self.assertEqual(self.mc1.get(TEST_KEY_2), None)

    def test_normal_lock(self):
        self._test_normal(Lock)

    def test_normal_rlock(self):
        self._test_normal(RLock)

    def test_normal_threadrlock(self):
        self._test_normal(ThreadRLock)

    def test_reentrant_rlock(self):
        self._test_reentrant(RLock)

    def test_reentrant_threadrlock(self):
        self._test_reentrant(ThreadRLock)

    def test_threaded_rlock(self):
        self.assertTrue(self._test_reentrant_thread(RLock))

    def test_threaded_threadrlock(self):
        self.assertFalse(self._test_reentrant_thread(ThreadRLock))

    def test_persistent_lock(self):
        self._test_persistent(Lock)

    def test_persistent_rlock(self):
        self._test_persistent(RLock)

    def test_persistent_threadrlock(self):
        self._test_persistent(ThreadRLock)

    def test_careful_release_lock(self):
        self._test_careful_release(Lock)

    def test_careful_release_rlock(self):
        self._test_careful_release(RLock)

    def test_careful_release_threadrlock(self):
        self._test_careful_release(ThreadRLock)


def locker((LockType, key, sleep_time)):
    lock = LockType(memcache.Client(TEST_HOSTS, cache_cas=True), key)
    lock.acquire()
    if sleep_time:
        time.sleep(sleep_time)
    lock.release()
    return lock.uid


class TestExptime(TestLock):
    def _test_exptime(self, LockType):
        exptime = 1
        locka = LockType(self.mc1, TEST_KEY_1, exptime=exptime)
        lockb = LockType(self.mc2, TEST_KEY_1)
        start = time.time()
        self.assertTrue(locka.acquire(False))
        self.assertTrue(lockb.acquire())
        interval = time.time() - start
        self.assertGreater(interval, exptime - 1)
        self.assertLess(interval, exptime + 1)
        lockb.release()
        # locka internal state is now desynchronised from memcached
        # it thinks it has the lock
        self.assertTrue(locka.locked(by_self=True))
        # ...but fails releasing it
        self.assertRaises(MemcacheLockReleaseError, locka.release)
        self.assertTrue(locka.locked(by_self=True))
        locka.resync()
        self.assertFalse(locka.locked(by_self=True))

    def test_exptime_lock(self):
        self._test_exptime(Lock)

    def test_exptime_rlock(self):
        self._test_exptime(RLock)

    def test_non_persistent_exptime_lock(self):
        self._test_exptime(partial(Lock, persistent=False))

    def test_non_persistent_exptime_rlock(self):
        self._test_exptime(partial(RLock, persistent=False))

    def test_exptime_threadrlock(self):
        self._test_exptime(ThreadRLock)


class TestTimeout(TestLock):
    lockb_backoff = 0
    lockb_interval = 0.05
    timeout = .05

    def _test_timeout(self, LockType):
        timeout = self.timeout
        locka = LockType(self.mc1, TEST_KEY_1)
        lockb = LockType(self.mc2, TEST_KEY_1, interval=self.lockb_interval,
            backoff=self.lockb_backoff)
        self.assertTrue(locka.acquire(False))
        start = time.time()
        self.assertFalse(lockb.acquire(timeout=timeout))
        interval = time.time() - start
        self.assertGreater(interval, timeout * .5)
        self.assertLess(interval, timeout * 1.5)
        locka.release()

    def test_timeout_lock(self):
        self._test_timeout(Lock)

    def test_timeout_rlock(self):
        self._test_timeout(RLock)

    def test_timeout_threadrlock(self):
        self._test_timeout(ThreadRLock)


class TestBackoffTimeout(TestTimeout):
    lockb_backoff = TestTimeout.timeout * 10


class TestIntervalTimeout(TestTimeout):
    lockb_interval = TestTimeout.timeout * 10


class TestIntervalBackoffTimeout(TestBackoffTimeout, TestIntervalTimeout):
    pass


class TestSwarm(TestLock):
    SWARM_SIZE = 30
    SLEEP_TIME = 0.001

    def _test_deadlock(self, LockType):

        try:
            pool = multiprocessing.Pool(processes=self.SWARM_SIZE)
        except OSError:
            raise unittest.SkipTest('multiprocessing.Pool call failed')
        self.addCleanup(self._cleanup_pool, pool)
        start = time.time()
        result = list(pool.imap_unordered(
            locker,
            [
                (LockType, TEST_KEY_1, self.SLEEP_TIME)
                for _ in xrange(self.SWARM_SIZE)
            ]
        ))  # list forces us to get results
        interval = time.time() - start

        self.assertGreater(interval, self.SLEEP_TIME * self.SWARM_SIZE)
        self.assertEqual(len(result), self.SWARM_SIZE)
        self.assertEqual(len(set(result)), self.SWARM_SIZE, result)

    @staticmethod
    def _cleanup_pool(pool):
        pool.close()
        pool.join()

    def test_deadlock_lock(self):
        self._test_deadlock(Lock)

    def test_deadlock_rlock(self):
        self._test_deadlock(RLock)

    def test_deadlock_threadrlock(self):
        self._test_deadlock(ThreadRLock)


class TestSlowSwarm(TestSwarm):
    SLEEP_TIME = 0.01


if __name__ == '__main__':
    unittest.main()
