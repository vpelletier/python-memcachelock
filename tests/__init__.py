"""These tests rely on an accessible memcache server"""

import thread
import threading
import time
import multiprocessing

import memcache
from memcachelock import RLock, Lock, ThreadRLock, LOCK_UID_KEY_SUFFIX

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


def locker((LockType, key, sleep_time)):
    lock = LockType(memcache.Client(TEST_HOSTS, cache_cas=True), key)
    lock.acquire()
    if sleep_time:
        time.sleep(sleep_time)
    lock.release()
    return None


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

    def test_exptime_lock(self):
        self._test_exptime(Lock)

    def test_exptime_rlock(self):
        self._test_exptime(RLock)

    def test_exptime_threadrlock(self):
        self._test_exptime(ThreadRLock)


class TestSwarm(TestLock):
    SWARM_SIZE = 30

    def _test_deadlock(self, LockType):
        SLEEP_TIME = 0.001

        try:
            pool = multiprocessing.Pool(processes=self.SWARM_SIZE)
        except OSError:
            raise unittest.SkipTest('multiprocessing.Pool call failed')
        start = time.time()
        list(pool.imap_unordered(
            locker,
            [
                (LockType, TEST_KEY_1, SLEEP_TIME)
                for _ in xrange(self.SWARM_SIZE)
            ]
        ))  # list forces us to get results
        interval = time.time() - start

        self.assertGreater(interval, SLEEP_TIME * self.SWARM_SIZE)
        pool.close()
        pool.join()

    def test_deadlock_lock(self):
        self._test_deadlock(Lock)

    def test_deadlock_rlock(self):
        self._test_deadlock(RLock)

    def test_deadlock_threadrlock(self):
        self._test_deadlock(ThreadRLock)


if __name__ == '__main__':
    unittest.main()
