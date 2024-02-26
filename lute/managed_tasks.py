from typing import Dict

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

# SFX
#####
CrystFELIndexer = Executor("IndexCrystFEL")
CrystFELIndexer.update_environment(
    {"PATH": "/sdf/group/lcls/ds/tools/XDS-INTEL64_Linux_x86_64"}
)
