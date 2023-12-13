"""Basic test Tasks for testing functionality.

Classes:
    Test(Task): Simplest test Task - runs a 10 iteration loop and returns a
        result.

    TestSocket(Task): Test Task which sends larger data to test socket IPC.
"""

__all__ = ["Test", "TestSocket"]
__author__ = "Gabriel Dorlhiac"

import time

import numpy as np

from .task import *
from ..io.config import *
from ..execution.ipc import PipeCommunicator, Message


class Test(Task):
    """Simple test Task to ensure subprocess and pipe-based IPC work."""

    def __init__(self, *, params: TaskParameters) -> None:
        super().__init__(params=params)

    def _run(self) -> None:
        for i in range(10):
            time.sleep(1)
            msg: Message = Message(contents=f"Test message {i}")
            self._report_to_executor(msg)

    def _post_run(self) -> None:
        self._result.summary = "Test Finished."
        self._result.task_status = TaskStatus.COMPLETED
        time.sleep(0.1)


class TestSocket(Task):
    """Simple test Task to ensure basic IPC over Unix sockets works."""

    def __init__(self, *, params: TaskParameters) -> None:
        super().__init__(params=params)

    def _run(self) -> None:
        for i in range(self._task_parameters.array_size):
            msg: Message = Message(contents=f"Sending array {i}")
            self._report_to_executor(msg)
            time.sleep(0.05)
            msg: Message = Message(
                contents=np.random.rand(self._task_parameters.array_size)
            )
            self._report_to_executor(msg)

    def _post_run(self) -> None:
        super()._post_run()
        self._result.summary = f"Sent {self._task_parameters.num_arrays} arrays"
        self._result.payload = np.random.rand(self._task_parameters.array_size)
        self._result.task_status = TaskStatus.COMPLETED
