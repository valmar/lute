"""Classes and utilities for communication between Executors and subprocesses.

Communicators manage message passing and parsing between subprocesses. They
maintain a limited public interface of "read" and "write" operations. Behind
this interface the methods of communication vary from serialization across
pipes to Unix sockets, etc. All communicators pass a single object called a
"Message" which contains an arbitrary "contents" field as well as an optional
"signal" field.


Classes:

"""

__all__ = [
    "Party",
    "Message",
    "Communicator",
    "PipeCommunicator",
    "LUTE_SIGNALS",
    "SocketCommunicator",
]
__author__ = "Gabriel Dorlhiac"

import _io
import os
import sys
import socket
import pickle
import subprocess
import threading
import queue
from typing import Optional, Any, Set
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum

LUTE_SIGNALS: Set[str] = {
    "NO_PICKLE_MODE",
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
    def __init__(self, party: Party = Party.TASK, use_pickle: bool = True) -> None:
        """Abstract Base Class for IPC Communicator objects.

        Args:
            party (Party): Which object (side/process) the Communicator is
                managing IPC for. I.e., is this the "Task" or "Executor" side.
            use_pickle (bool): Whether to serialize data using pickle prior to
                sending it.
        """
        self._party = party
        self._use_pickle = use_pickle
        self.desc = "Communicator abstract base class."

    @abstractmethod
    def read(self, stdout: _io.BufferedReader, stderr: _io.BufferedReader) -> Message:
        """Method for reading data through the communication mechanism."""
        ...

    @abstractmethod
    def write(self, msg: Message) -> None:
        """Method for sending data through the communication mechanism."""
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

    def __init__(self, party: Party = Party.TASK, use_pickle: bool = True) -> None:
        """IPC through pipes.

        Arbitrary objects may be transmitted using pickle to serialize the data.
        If pickle is not used

        Args:
            party (Party): Which object (side/process) the Communicator is
                managing IPC for. I.e., is this the "Task" or "Executor" side.
            use_pickle (bool): Whether to serialize data using Pickle prior to
                sending it. If False, data is assumed to be text whi
        """
        super().__init__(party=party, use_pickle=use_pickle)
        self.desc = "Communicates through stderr and stdout using pickle."

    def read(self, proc: subprocess.Popen) -> Message:
        """Read from stdout and stderr.

        Args:
            proc (subprocess.Popen): The process to read from.

        Returns:
            msg (Message): The message read, containing contents and signal.
        """
        if self._use_pickle:
            signal: str = proc.stderr.readline().decode()
            raw_contents: bytes = proc.stdout.read()
            if raw_contents:
                try:
                    contents: Any = pickle.loads(raw_contents)
                except pickle.UnpicklingError as err:
                    # Can occur if Task switches to unpickled mode before the
                    # Executor can
                    self._use_pickle = False
                    contents: str = raw_contents.decode()
            else:
                contents: str  = ""
        else:
            signal: str = proc.stderr.read()
            contents: str = proc.stdout.readline().decode()

            if signal and signal not in LUTE_SIGNALS:
                # Some tasks write on stderr
                # If the signal channel has "non-signal" info, add it to
                # contents
                contents += signal
                signal = ""
        return Message(contents=contents, signal=signal)

    def write(self, msg: Message) -> None:
        """Write to stdout and stderr.

         The signal component is sent to `stderr` while the contents of the
         Message are sent to `stdout`.

        Args:
            msg (Message): The Message to send.
        """
        if self._use_pickle:
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
        else:
            raw_signal: str
            if msg.signal:
                raw_signal = msg.signal
            else:
                raw_signal = ""

            raw_contents: str
            if isinstance(msg.contents, str):
                raw_contents = msg.contents
            elif msg.contents is None:
                raw_contents = ""
            else:
                raise ValueError(
                    f"Cannot send msg contents of type: {type(msg.contents)} when not using pickle!"
                )
            sys.stderr.write(raw_signal)
            sys.stdout.write(raw_contents)

class SocketCommunicator(Communicator):
    """Provides communication over Unix sockets.

    The path to the Unix socket is defined by the environment variable:
                      `LUTE_SOCKET=/path/to/socket`
    This class assumes proper permissions and that this above environment
    variable has been defined. The `Task` is configured as what would commonly
    be referred to as the `client`, while the `Executor` is configured as the
    server. The Executor continuosly monitors for connections and appends any
    Messages that are received to a queue. Read requests retrieve Messages from
    the queue. Task-side Communicators are fleeting so they open a connection,
    send data, and immediately close and clean up.
    """

    MAX_QUEUE: int = 1024
    """
    Maximum size of the Message queue.
    """
    READ_TIMEOUT: float = 0.5
    """
    Maximum time to wait to retrieve an item from the Message queue.
    """

    def __init__(self, party: Party = Party.TASK) -> None:
        """IPC over a Unix socket.

        Unlike with the PipeCommunicator, pickle is always used to send data
        through the socket.

        Args:
            party (Party): Which object (side/process) the Communicator is
                managing IPC for. I.e., is this the "Task" or "Executor" side.
        """
        super().__init__(party=party)
        self.desc: str = "Communicates through a Unix socket."

        self._data_socket: socket.socket = self._create_socket()

        # Only the Executor needs a thread...
        if self._party == Party.EXECUTOR:
            self._comm_thread: threading.Thread = threading.Thread(target=self._listen_socket)
            self._comm_thread.start()
            self._is_listening: bool = True
            self._queue: queue.Queue = queue.Queue(maxsize=SocketCommunicator.MAX_QUEUE)
            #self._comm_thread.join()
        elif self._party == Party.TASK:
            pass
        else:
            raise ValueError(
                f"Unknown Party requested! {self._party} is not implemented!"
            )

    def read(self, proc: subprocess.Popen) -> Message:
        """Read data from a queue.

        Data is from a continuously monitored socket, where it is added to a
        queue. This method reads from the queue in FIFO fashion.

        Args:
            proc (subprocess.Popen): The process to read from. Provided for
                compatibility with other Communicator subtypes. Is ignored.

        Returns:
             msg (Message): The message read, containing contents and signal.
        """
        msg: Message
        try:
            msg = self._queue.get(timeout=SocketCommunicator.READ_TIMEOUT)
        except queue.Empty:
            msg = Message()

        return msg

    def write(self, msg: Message) -> None:
        """Send a single Message.

        The entire Message (signal and contents) is serialized and sent through
        a connection over Unix socket.

        Args:
            msg (Message): The Message to send.
        """
        self._write_socket(msg)

    def _create_socket(self) -> socket.socket:
        """Returns a socket object.

        Returns:
            data_socket (socket.socket): Unix socket object.
        """
        socket_path: str = os.environ["LUTE_SOCKET"]
        data_socket: socket.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        if self._party == Party.EXECUTOR:
            data_socket.bind(socket_path)
            data_socket.listen()
        elif self._party == Party.TASK:
            data_socket.connect(socket_path)

        return data_socket

    def _listen_socket(self) -> None:
        """Listens to a socket on the 'server' (Executor) side.

        Received data is packaged and added to a queue.
        """
        #self._data_socket: socket.socket = self._create_socket()

        while self._is_listening:
            connection, _ = self._data_socket.accept()
            full_data: bytes = b""
            while True:
                data: bytes = connection.recv(1024)
                if data:
                    full_data += data
                else:
                    break
            msg: Message = pickle.loads(full_data)
            self._queue.put(msg)

    def _write_socket(self, msg: Message) -> None:
        """Sends data over a socket from the 'client' (Task) side.

        Communicator objects on the Task-side are fleeting, so a socket is
        opened, data is sent, and then the connection and socket are cleaned up.
        """
        self._data_socket: socket.socket = self._create_socket()
        self._data_socket.sendall(pickle.dumps(msg))

        self._clean_up()

    def _clean_up(self) -> None:
        """Clean up connections."""
        # Check the object exists in case the Communicator is cleaned up before
        # opening any connections
        if hasattr(self, "_data_socket"):
            socket_path: str = self._data_socket.getsockname()
            self._data_socket.close()

            if self._party == Party.EXECUTOR:
                os.unlink(socket_path)

    def __del__(self):
        self._clean_up()
