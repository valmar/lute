"""Basic test Tasks for testing functionality.

Classes:
    Test(Task): Simplest test Task - runs a 10 iteration loop and returns a
        result.
"""
import time

from .task import *
from ..io.config import *
from ..execution.ipc import PipeCommunicator, Message


class Test(Task):
    """Simple test task to ensure subprocess and IPC work."""

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
