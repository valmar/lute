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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Dict
from enum import Enum
import os

from ..io.config import TaskParameters


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
        self._status: TaskStatus = TaskStatus.PENDING
        self._result: TaskResult = TaskResult(
            task_name=self.name, task_status=self.status, summary="PENDING", payload=""
        )
        self._task_parameters = params

    def run(self) -> None:
        """Calls the analysis routines and any pre/post task functions.

        This method is part of the public API and should not need to be modified
        in any subclasses.
        """
        self._pre_run()
        self._run()
        self._post_run()

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

    @property
    def status(self) -> TaskStatus:
        """TaskStatus: The current status of the Task. Read-only"""
        return self._status

    def __call__(self) -> None:
        self.run()


class BinaryTask(Task):
    """A `Task` interface to analysis with binary executables."""

    def __init__(self, *, params: TaskParameters, flag_names: Dict[str, str]) -> None:
        """Initialize a Task.

        Args:
            params (TaskParameters): Parameters needed to properly configure
                the analysis task. `Task`s of this type MUST include the name
                of a binary to run and any arguments which should be passed to
                it (as would be done via command line).

            flag_names (Dict[str, str]): A dictionary of friendly names and
                their corresponding command-line flags. E.g. a binary executable
                which takes a number of cores flag may have a dictionary entry
                that looks like:
                    * flag_names = { "ncores" : "-n" }
                flag_names must match the corresponding parameter names.
        """
        super().__init__(params=params)
        self._flag_names: Dict[str, str] = flag_names
        self._cmd = self._task_parameters.pop("executable")
        self._args_list: List[str] = []

    def _pre_run(self):
        """Prepare the list of flags and arguments to be executed."""
        super()._pre_run()
        for param, value in self._task_parameters:
            self._args_list.append(self._flag_names[str(param)])
            self._args_list.append(str(value))

    def _run(self):
        """Execute the new program by replacing the current process."""
        os.execvp(file=self._cmd, args=self._args_list)
