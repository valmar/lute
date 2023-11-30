"""Classes and utilities for communication between Executors and subprocesses.

Communicators manage message passing and parsing between subprocesses. They
maintain a limited public interface of "read" and "write" operations. Behind
this interface the methods of communication vary from serialization across
pipes to Unix sockets, etc. All communicators pass a single object called a
"Message" which contains an arbitrary "contents" field as well as an optional
"signal" field.


Classes:

"""

import _io
import os
import sys
import socket
import pickle
import subprocess
from typing import Optional, Any, Set
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum

__all__ = [
    "Party",
    "Message",
    "Communicator",
    "PipeCommunicator",
    "LUTE_SIGNALS",
    "SocketCommunicator",
]
__author__ = "Gabriel Dorlhiac"

LUTE_SIGNALS: Set[str] = {
    "TASK_STARTED",
    "TASK_FAILED",
    "TASK_STOPPED",
    "TASK_DONE",
    "TASK_CANCELLED",
    "TASK_RESULT",
}


class Party(Enum):
    """Identifier for which party (side/end) is using a communicator.

    For some types of communication streams there may be different interfaces
    depending on which side of the communicator you are on. This enum is used
    by the communicator to determine which interface to use.
    """

    TASK = 0
    """
    The Task (client) side.
    """
    EXECUTOR = 1
    """
    The Executor (server) side.
    """


@dataclass
class Message:
    contents: Optional[Any] = None
    signal: Optional[str] = None


class Communicator(ABC):
    def __init__(self, party: Party = Party.TASK) -> None:
        self._party = party
        self.desc = "Communicator abstract base class."

    @abstractmethod
    def read(self, stdout: _io.BufferedReader, stderr: _io.BufferedReader) -> Message:
        ...

    @abstractmethod
    def write(self, msg: Message) -> None:
        ...

    def __str__(self):
        name: str = str(type(self)).split("'")[1].split(".")[-1]
        return f"{name}: {self.desc}"

    def __repr__(self):
        return self.__str__()


class PipeCommunicator(Communicator):
    """Provides communication through pipes over stderr/stdout.

    The implementation of this communicator has reading and writing ocurring
    on stderr and stdout. In general the `Task` will be writing while the
    `Executor` will be reading. `stderr` is used for sending signals.
    """

    def __init__(self, party: Party = Party.TASK) -> None:
        super().__init__(party=party)
        self.desc = "Communicates through stderr and stdout using pickle."

    def read(self, proc: subprocess.Popen) -> Message:
        """Read from stdout and stderr.

        Args:
            proc (subprocess.Popen): The process to read from.
        """
        signal: str = proc.stderr.readline().decode()
        raw_contents: bytes = proc.stdout.read()
        if raw_contents:
            contents: Any = pickle.loads(raw_contents)
        else:
            contents = ""

        return Message(contents=contents, signal=signal)

    def write(self, msg: Message) -> None:
        signal: bytes
        if msg.signal:
            signal = msg.signal.encode()
        else:
            signal = b""

        contents: bytes = pickle.dumps(msg.contents)

        sys.stderr.buffer.write(signal)
        sys.stdout.buffer.write(contents)

        sys.stderr.buffer.flush()
        sys.stdout.buffer.flush()


class SocketCommunicator(Communicator):
    """Provides communication over Unix sockets.

    The path to the Unix socket is defined by the environment variable:
                      `LUTE_SOCKET=/path/to/socket`
    This class assumes proper permissions and that this above environment
    variable has been defined. The `Task` is configured as what would commonly
    be referred to as the `client`, while the `Executor` is configured as the
    server.
    """

    def __init__(self, party: Party = Party.TASK) -> None:
        raise NotImplementedError

    def read(self, proc: subprocess.Popen) -> Message:
        ...

    def write(self, msg: Message) -> None:
        ...

    def __del__(self):
        # important to clean up socket
        ...
