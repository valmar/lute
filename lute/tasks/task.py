"""Base classes for implementing analysis tasks.

Classes:
    Task: Abstract base class from which all analysis tasks are derived.

    TaskResult: Output of a specific analysis task.

    TaskStatus: Enumeration of possible Task statuses (running, pending, failed,
        etc.).

    BinaryTask: Class to run a third-party executable binary as a `Task`.
"""

__all__ = ["Task", "TaskResult", "TaskStatus", "BinaryTask"]
__author__ = "Gabriel Dorlhiac"

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List
from enum import Enum
import os
import sys

from ..io.config import TaskParameters
from ..execution.ipc import *


class TaskStatus(Enum):
    """Possible Task statuses."""

    PENDING = 0
    """
    Task has yet to run. Is Queued, or waiting for prior tasks.
    """
    RUNNING = 1
    """
    Task is in the process of execution.
    """
    COMPLETED = 2
    """
    Task has completed without fatal errors.
    """
    FAILED = 3
    """
    Task encountered a fatal error.
    """
    STOPPED = 4
    """
    Task was, potentially temporarily, stopped/suspended.
    """
    CANCELLED = 5
    """
    Task was cancelled prior to completion or failure.
    """
    TIMEDOUT = 6
    """
    Task did not reach completion due to timeout.
    """


@dataclass
class TaskResult:
    """Class for storing the result of a Task's execution with metadata.

    Attributes:
        task_name (str): Name of the associated task which produced it.

        task_status (TaskStatus): Status of associated task.

        summary (str): Short message/summary associated with the result.

        payload (Any): Actual result. May be data in any format.
    """

    task_name: str
    task_status: TaskStatus
    summary: str
    payload: Any


class Task(ABC):
    """Abstract base class for analysis tasks.

    Attributes:
        name (str): The name of the Task.
    """

    def __init__(self, *, params: TaskParameters) -> None:
        """Initialize a Task.

        Args:
            params (TaskParameters): Parameters needed to properly configure
                the analysis task. These are NOT related to execution parameters
                (number of cores, etc), except, potentially, in case of binary
                executable sub-classes.
        """
        self.name: str = str(type(self)).split("'")[1].split(".")[-1]
        self._result: TaskResult = TaskResult(
            task_name=self.name,
            task_status=TaskStatus.PENDING,
            summary="PENDING",
            payload="",
        )
        self._task_parameters = params

    def run(self) -> None:
        """Calls the analysis routines and any pre/post task functions.

        This method is part of the public API and should not need to be modified
        in any subclasses.
        """
        self._signal_start()
        self._pre_run()
        self._run()
        self._post_run()
        self._signal_result()

    @abstractmethod
    def _run(self) -> None:
        """Actual analysis to run. Overridden by subclasses.

        Separating the calling API from the implementation allows `run` to
        have pre and post task functionality embedded easily into a single
        function call.
        """
        ...

    def _pre_run(self) -> None:
        """Code to run BEFORE the main analysis takes place.

        This function may, or may not, be employed by subclasses.
        """
        ...

    def _post_run(self) -> None:
        """Code to run AFTER the main analysis takes place.

        This function may, or may not, be employed by subclasses.
        """
        ...

    @property
    def result(self) -> TaskResult:
        """TaskResult: Read-only Task Result information."""
        return self._result

    def __call__(self) -> None:
        self.run()

    def _signal_start(self) -> None:
        """Send the signal that the Task will begin shortly."""
        start_msg: Message = Message(
            contents=self._task_parameters, signal="TASK_STARTED"
        )
        self._result.task_status = TaskStatus.RUNNING
        self._report_to_executor(start_msg)

    def _signal_result(self) -> None:
        """Send the signal that results are ready along with the results."""
        signal: str = "TASK_RESULT"
        results_msg: Message = Message(contents=self.result, signal=signal)
        self._report_to_executor(results_msg)
        time.sleep(0.1)

    def _report_to_executor(self, msg: Message) -> None:
        """Send a message to the Executor.

        Details of `Communicator` choice are hidden from the caller.
        """
        communicator: Communicator
        if sys.getsizeof(msg) > 6e4:
            communicator = SocketCommunicator()
        else:
            communicator = PipeCommunicator()
        communicator.write(msg)


class BinaryTask(Task):
    """A `Task` interface to analysis with binary executables."""

    def __init__(self, *, params: TaskParameters) -> None:
        """Initialize a Task.

        Args:
            params (TaskParameters): Parameters needed to properly configure
                the analysis task. `Task`s of this type MUST include the name
                of a binary to run and any arguments which should be passed to
                it (as would be done via command line). The binary is included
                with the parameter `executable`. All other parameter names are
                assumed to be the long/extended names of the flag passed on the
                command line:
                    * `arg_name = 3` is converted to `--arg_name 3`
                Positional arguments can be included with `_argN` where `N` is
                any integer:
                    * `_arg1 = 3` is converted to `3`
        """
        super().__init__(params=params)
        self._cmd = self._task_parameters.executable
        self._args_list: List[str] = []

    def _pre_run(self):
        """Prepare the list of flags and arguments to be executed."""
        super()._pre_run()
        # We assume no compound/nested parameters for these Task types
        # I.e. no parameters like: param = {"a": 1, "b": 2}, etc..
        for param, value in self._task_parameters.dict().items():
            if param == "executable":
                continue
            if "_arg" in param:
                # _arg indicates a positional argument, so no flag
                self._args_list.append(f"{value}")
            else:
                self._args_list.append(f"--{param}")
                self._args_list.append(f"{value}")

    def _run(self):
        """Execute the new program by replacing the current process."""
        os.execvp(file=self._cmd, args=self._args_list)
