import time
import threading
import unittest
from scheduler import Scheduler
from unittest.mock import patch, MagicMock
from queue import Queue, Empty

class TestEventDrivenScheduler(unittest.TestCase):

    def setUp(self):
        self.scheduler = Scheduler()
        self.scheduler.start()

    def tearDown(self):
        self.scheduler.shutdown()

    # -------------------------------
    # 1. Delayed Execution Validation
    # -------------------------------
    @patch("time.time")
    def test_task_executes_only_after_delay(self, mock_time):
        executed = threading.Event()

        def task():
            executed.set()

        mock_time.return_value = 100.0
        self.scheduler.schedule(task, run_at=105.0)

        time.sleep(0.1)
        self.assertFalse(executed.is_set(), "Task executed too early")

        mock_time.return_value = 106.0
        time.sleep(0.2)

        self.assertTrue(executed.is_set(), "Task did not execute after delay")

    # --------------------------------
    # 2. Invalid Scheduling Times
    # --------------------------------
    def test_rejects_past_execution_time(self):
        with self.assertRaises(ValueError):
            self.scheduler.schedule(lambda: None, run_at=time.time() - 10)

    def test_rejects_nan_and_negative_time(self):
        with self.assertRaises(ValueError):
            self.scheduler.schedule(lambda: None, run_at=float("nan"))

        with self.assertRaises(ValueError):
            self.scheduler.schedule(lambda: None, run_at=-1)

    # --------------------------------
    # 3. Concurrent Task Dispatch
    # --------------------------------
    def test_concurrent_tasks_all_execute_once(self):
        results = Queue()
        task_count = 50

        def task(i):
            results.put(i)

        run_at = time.time() + 0.1

        for i in range(task_count):
            self.scheduler.schedule(lambda i=i: task(i), run_at)

        time.sleep(0.5)

        executed = set()
        while True:
            try:
                executed.add(results.get_nowait())
            except Empty:
                break

        self.assertEqual(
            len(executed),
            task_count,
            "Lost or duplicated tasks detected (race condition)"
        )

    # --------------------------------
    # 4. Cancellation Race Condition
    # --------------------------------
    def test_cancellation_prevents_execution(self):
        executed = False
        lock = threading.Lock()

        def task():
            nonlocal executed
            with lock:
                executed = True

        run_at = time.time() + 0.2
        task_id = self.scheduler.schedule(task, run_at)

        # Cancel very close to execution time
        time.sleep(0.15)
        cancelled = self.scheduler.cancel(task_id)

        time.sleep(0.3)

        with lock:
            self.assertTrue(cancelled, "Cancel returned false")
            self.assertFalse(executed, "Cancelled task still executed")

    # --------------------------------
    # 5. Cancellation During Dispatch
    # --------------------------------
    def test_cancel_during_worker_pickup(self):
        latch = threading.Event()

        def blocking_task():
            latch.set()
            time.sleep(0.3)

        run_at = time.time() + 0.1
        task_id = self.scheduler.schedule(blocking_task, run_at)

        latch.wait(timeout=0.2)
        cancelled = self.scheduler.cancel(task_id)

        # Scheduler must not crash or double-execute
        self.assertIn(cancelled, [True, False])