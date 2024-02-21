from .io.config import *
from .execution.executor import *

# Tests
#######
Tester = Executor("Test")
BinaryTester = Executor("TestBinary")
SocketTester = Executor("TestSocket")
WriteTester = Executor("TestWriteOutput")
ReadTester = Executor("TestReadOutput")

# SmallData-related
###################
SmallDataProducer = Executor("SubmitSMD")
