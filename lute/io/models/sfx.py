"""Models for serial femtosecond crystallography Tasks.

Classes:
    IndexCrystFEL(BaseBinaryParameters): Perform indexing of hits/peaks using
        CrystFEL's `indexamajig`.
"""
from typing import Union, List, Optional, Dict, Any

from pydantic import (
    BaseModel,
    AnyUrl,
    PositiveInt,
    PositiveFloat,
    NonNegativeInt,
    Field,
    conint,
    validator,
)

from .base import TaskParameters, BaseBinaryParameters


class IndexCrystFEL(BaseBinaryParameters):
    """Parameters for CrystFEL's `indexamajig`."""

    class Config(BaseBinaryParameters.Config):
        long_flags_use_eq: bool = True
        """Whether long command-line arguments are passed like `-long=arg`."""

    executable: str = Field(
        "/sdf/group/lcls/ds/tools/crystfel/0.10.2/bin/indexamajig",
        description="CrystFEL's indexing binary.",
        flag_type="",
    )
    # Basic options
    infile: Optional[str] = Field(
        "", description="Path to input file.", flag_type="-", rename_param="i"
    )
    outfile: str = Field(
        description="Path to output file.", flag_type="-", rename_param="o"
    )
    geometry: str = Field(
        description="Path to geometry file.", flag_type="-", rename_param="g"
    )
    zmq_input: Optional[str] = Field(
        description="ZMQ address to receive data over. `input` and `zmq-input` are mutually exclusive",
        flag_type="--",
        rename_param="zmq-input",
    )
    zmq_subscribe: Optional[str] = Field(  # Can be used multiple times...
        description="Subscribe to ZMQ message of type `tag`",
        flag_type="--",
        rename_param="zmq-subscribe",
    )
    zmq_request: Optional[AnyUrl] = Field(
        description="Request new data over ZMQ by sending this value",
        flag_type="--",
        rename_param="zmq-request",
    )
    asapo_endpoint: Optional[str] = Field(
        description="ASAP::O endpoint. zmq-input and this are mutually exclusive.",
        flag_type="--",
        rename_param="asapo-endpoint",
    )
    asapo_token: Optional[str] = Field(
        description="ASAP::O authentication token.",
        flag_type="--",
        rename_param="asapo-token",
    )
    asapo_beamtime: Optional[str] = Field(
        description="ASAP::O beatime.",
        flag_type="--",
        rename_param="asapo-beamtime",
    )
    asapo_source: Optional[str] = Field(
        description="ASAP::O data source.",
        flag_type="--",
        rename_param="asapo-source",
    )
    asapo_group: Optional[str] = Field(
        description="ASAP::O consumer group.",
        flag_type="--",
        rename_param="asapo-group",
    )
    asapo_stream: Optional[str] = Field(
        description="ASAP::O stream.",
        flag_type="--",
        rename_param="asapo-stream",
    )
    asapo_wait_for_stream: Optional[str] = Field(
        description="If ASAP::O stream does not exist, wait for it to appear.",
        flag_type="--",
        rename_param="asapo-wait-for-stream",
    )
    data_format: Optional[str] = Field(
        description="Specify format for ZMQ or ASAP::O. `msgpack`, `hdf5` or `seedee`.",
        flag_type="--",
        rename_param="data-format",
    )
    basename: bool = Field(
        False,
        description="Remove directory parts of filenames. Acts before prefix if prefix also given.",
        flag_type="--",
    )
    prefix: Optional[str] = Field(
        description="Add a prefix to the filenames from the infile argument.",
        flag_type="--",
        rename_param="asapo-stream",
    )
    nthreads: PositiveInt = Field(
        1,
        description="Number of threads to use. See also `max_indexer_threads`.",
        flag_type="-",
        rename_param="j",
    )
    no_check_prefix: bool = Field(
        bool,
        description="Don't attempt to correct the prefix if it seems incorrect.",
        flag_type="--",
        rename_param="no-check-prefix",
    )
    highres: Optional[float] = Field(
        description="Mark all pixels greater than `x` has bad.", flag_type="--"
    )
    profile: bool = Field(
        False, description="Display timing data to monitor performance.", flag_type="--"
    )
    temp_dir: Optional[str] = Field(
        description="Specify a path for the temp files folder.",
        flag_type="--",
        rename_param="temp-dir",
    )
    wait_for_file: conint(gt=-2) = Field(
        0,
        description="Wait at most `x` seconds for a file to be created. A value of -1 means wait forever.",
        flag_type="--",
        rename_param="wait-for-file",
    )
    no_image_data: bool = Field(
        False,
        description="Load only the metadata, no iamges. Can check indexability without high data requirements.",
        flag_type="--",
        rename_param="no-image-data",
    )
    # Peak-finding options
    # ....
    # Indexing options
    indexing: Optional[str] = Field(
        description="Comma-separated list of supported indexing algorithms to use. Default is to automatically detect.",
        flag_type="--",
    )
    cell_file: Optional[str] = Field(
        description="Path to a file containing unit cell information (PDB or CrystFEL format).",
        flag_type="-",
        rename_param="p",
    )
    tolerance: str = Field(
        description=(
            "Tolerances (in percent) for unit cell comparison. "
            "Comma-separated list a,b,c,angle. Default=5,5,5,1.5"
        ),
        flag_type="--",
    )
    no_check_cell: bool = Field(
        False,
        description="Do not check cell parameters against unit cell. Replaces '-raw' method.",
        flag_type="--",
        rename_param="no-check-cell",
    )
    no_check_peaks: bool = Field(
        False,
        description="Do not verify peaks are accounted for by solution.",
        flag_type="--",
        rename_param="no-check-peaks",
    )
    multi: bool = Field(
        False, description="Enable multi-lattice indexing.", flag_type="--"
    )
    wavelength_estimate: Optional[float] = Field(
        description="Estimate for X-ray wavelength. Required for some methods.",
        flag_type="--",
        rename_param="wavelength-estimate",
    )
    camera_length_estimate: Optional[float] = Field(
        description="Estimate for camera distance. Required for some methods.",
        flag_type="--",
        rename_param="camera-length-estimate",
    )
    max_indexer_threads: PositiveInt = Field(
        1,
        description="Some indexing algos can use multiple threads. In addition to image-based.",
        flag_type="--",
        rename_param="max-indexer-threads",
    )
    no_retry: bool = Field(
        False,
        description="Do not remove weak peaks and try again.",
        flag_type="--",
        rename_param="no-retry",
    )
    no_refine: bool = Field(
        False,
        description="Skip refinement step.",
        flag_type="--",
        rename_param="no-refine",
    )
    # TakeTwo specific parameters
    taketwo_member_threshold: PositiveInt = Field(
        20,
        description="Minimum number of vectors to consider.",
        flag_type="--",
        rename_param="taketwo-member-threshold",
    )
    taketwo_len_tolerance: PositiveFloat = Field(
        0.001,
        description="TakeTwo length tolerance in Angstroms.",
        flag_type="--",
        rename_param="taketwo-len-tolerance",
    )
    taketwo_angle_tolerance: PositiveFloat = Field(
        0.6,
        description="TakeTwo angle tolerance in degrees.",
        flag_type="--",
        rename_param="taketwo-angle-tolerance",
    )
    taketwo_trace_tolerance: PositiveFloat = Field(
        3,
        description="Matrix trace tolerance in degrees.",
        flag_type="--",
        rename_param="taketwo-trace-tolerance",
    )
    # Felix-specific parameters
    # felix_domega
    # felix-fraction-max-visits
    # felix-max-internal-angle
    # felix-max-uniqueness
    # felix-min-completeness
    # felix-min-visits
    # felix-num-voxels
    # felix-sigma
    # felix-tthrange-max
    # felix-tthrange-min
    # XGANDALF-specific parameters
    xgandalf_sampling_pitch: NonNegativeInt = Field(
        6,
        description="Density of reciprocal space sampling.",
        flag_type="--",
        rename_param="xgandalf-sampling-pitch",
    )
    xgandalf_grad_desc_iterations: NonNegativeInt = Field(
        4,
        description="Number of gradient descent iterations.",
        flag_type="--",
        rename_param="xgandalf-grad-desc-iterations",
    )
    xgandalf_tolerance: PositiveFloat = Field(
        0.02,
        description="Relative tolerance of lattice vectors",
        flag_type="--",
        rename_param="xgandalf-tolerance",
    )
    xgandalf_no_deviation_from_provided_cell: Optional[bool] = Field(
        description="Found unit cell must match provided.",
        flag_type="--",
        rename_param="xgandalf-no-deviation-from-provided-cell",
    )
    xgandalf_min_lattice_vector_length: PositiveFloat = Field(
        30,
        description="Minimum possible lattice length.",
        flag_type="--",
        rename_param="xgandalf-min-lattice-vector-length",
    )
    xgandalf_max_lattice_vector_length = Field(
        250,
        description="Minimum possible lattice length.",
        flag_type="--",
        rename_param="xgandalf-max-lattice-vector-length",
    )
    xgandalf_max_peaks: PositiveInt = Field(
        250,
        description="Maximum number of peaks to use for indexing.",
        flag_type="--",
        rename_param="xgandalf-max-peaks",
    )
    xgandalf_fast_execution: bool = Field(
        False,
        description="Shortcut to set sampling-pitch=2, and grad-desc-iterations=3.",
        flag_type="--",
        rename_param="xgandalf-fast-execution",
    )
    # pinkIndexer parameters
