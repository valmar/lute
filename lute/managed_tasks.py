from typing import Dict

from .io.config import *
from .execution.executor import *

# Tests
#######
Tester: Executor = Executor("Test")
BinaryTester: Executor = Executor("TestBinary")
SocketTester: Executor = Executor("TestSocket")
WriteTester: Executor = Executor("TestWriteOutput")
ReadTester: Executor = Executor("TestReadOutput")

# SmallData-related
###################
SmallDataProducer: Executor = Executor("SubmitSMD")

# SFX
#####
CrystFELIndexer: Executor = Executor("IndexCrystFEL")
CrystFELIndexer.update_environment(
    {"PATH": "/sdf/group/lcls/ds/tools/XDS-INTEL64_Linux_x86_64"}
)
PartialtorMerger: Executor = Executor("MergePartialator")
HKLComparer: Executor = Executor("CompareHKL")  # For figures of merit
HKLManipulator: Executor = Executor("ManipulateHKL")  # For hkl->mtz, but can do more
