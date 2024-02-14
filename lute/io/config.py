"""Machinary for the IO of configuration YAML files and their validation.

Classes:
    TaskParameters(BaseModel): Base class for Task parameters. Subclasses
        specify a model of parameters and their types for validation.

    FindOverlapXSSParameters(TaskParameters): Parameter model for the
        FindOverlapXSS Task.

Functions:
    parse_config(taskname: str, config_path: str) -> TaskParameters: Parse a
        configuration file and return a TaskParameters object of validated
        parameters for a specific Task. Raises an exception if the provided
        configuration does not match the expected model.

Exceptions:
    ValidationError: Error raised by pydantic during data validation. (From
        Pydantic)
"""

__all__ = ["parse_config", "TaskParameters"]
__author__ = "Gabriel Dorlhiac"

import os
from abc import ABC
from typing import List, Dict, Iterator, Dict, Any, Union, Optional

import yaml
import yaml
from pydantic import (
    BaseModel,
    BaseSettings,
    ValidationError,
    HttpUrl,
    PositiveInt,
    NonNegativeInt,
    Field,
    conint,
    root_validator,
    validator,
)
from pydantic.dataclasses import dataclass

from .db import read_latest_db_entry


# Parameter models
##################
# Parameter models
##################
class AnalysisHeader(BaseModel):
    """Header information for LUTE analysis runs."""

    title: str = Field(
        "LUTE Task Configuration",
        description="Description of the configuration or experiment.",
    )
    experiment: str = Field("EXPX00000", description="Experiment.")
    run: Union[str, int] = Field(
        os.environ.get("RUN", ""), description="Data acquisition run."
    )
    date: str = Field("1970/01/01", description="Start date of analysis.")
    lute_version: Union[float, str] = Field(
        0.1, description="Version of LUTE used for analysis."
    )
    task_timeout: PositiveInt = Field(
        600,
        description=(
            "Time in seconds until a task times out. Should be slightly shorter"
            " than job timeout if using a job manager (e.g. SLURM)."
        ),
    )
    work_dir: str = Field("", description="Main working directory for LUTE.")

    @validator("work_dir", always=True)
    def validate_work_dir(cls, work_dir: str, values: Dict[str, Any]) -> str:
        if work_dir == "":
            work_dir = (
                f"/sdf/data/lcls/ds/{values['experiment'][:3]}/"
                f"{values['experiment']}/scratch"
            )
        return work_dir


class TaskParameters(BaseSettings):
    """Base class for models of task parameters to be validated.

    Parameters are read from a configuration YAML file and validated against
    subclasses of this type in order to ensure that both all parameters are
    present, and that the parameters are of the correct type.

    Note:
        Pydantic is used for data validation. Pydantic does not perform "strict"
        validation by default. Parameter values may be cast to conform with the
        model specified by the subclass definition if it is possible to do so.
        Consider whether this may cause issues (e.g. if a float is cast to an
        int).
    """

    class Config:
        env_prefix = "LUTE_"
        extra: str = "allow"
        underscore_attrs_are_private: bool = True
        copy_on_model_validation: str = "deep"
        allow_inf_nan: bool = False

    lute_config: AnalysisHeader = AnalysisHeader()


@dataclass
class ThirdPartyParameters:
    """Class for representing parameters for third party configuration files.

    These parameters can represent arbitrary data types and are used in
    conjunction with templates for modifying third party configuration files
    from the single LUTE YAML. Due to the storage of arbitrary data types, and
    the use of a template file, a single instance of this class can hold from a
    single template variable to an entire configuration file. The data parsing
    is done by jinja using the complementary template.
    All data is stored in the single model variable `params.`

    The pydantic "dataclass" is used over the BaseModel/Settings to allow
    positional argument instantiation of the `params` Field.
    """

    params: Any


class TemplateConfig(BaseModel):
    """Parameters used for templating of third party configuration files."""

    template_dir: str
    output_dir: str


class BaseBinaryParameters(TaskParameters):
    """Base class for third party task parameters.

    Contains special validators for extra arguments and handling of parameters
    used for filling in third party configuration files.
    """

    class Config:
        extra: str = "allow"

    # lute_template_cfg: TemplateConfig

    @root_validator(pre=False)
    def extra_fields_to_thirdparty(cls, values):
        for key in values:
            if key not in cls.__fields__:
                values[key] = ThirdPartyParameters(values[key])

        return values


# Test Task models
##################
class TestParameters(TaskParameters):
    """Parameters for the test Task `Test`."""

    float_var: float = 0.01
    str_var: str = "test"

    class CompoundVar(BaseModel):
        int_var: int = 1
        dict_var: Dict[str, str] = {"a": "b"}

    compound_var: CompoundVar


class TestBinaryParameters(BaseBinaryParameters):
    executable: str = "/sdf/home/d/dorlhiac/test_tasks/test_threads"
    p_arg1: int = 1


class TestSocketParameters(TaskParameters):
    array_size: int = 10000
    num_arrays: int = 10


class TestWriteOutputParameters(TaskParameters):
    outfile_name: str = Field(
        "test_output.txt", description="Outfile name without full path."
    )
    num_vals: int = Field(100, description='Number of values to "process"')


class TestReadOutputParameters(TaskParameters):
    in_file: str = Field("", description="File to read in. (Full path)")

    @validator("in_file", always=True)
    def validate_work_dir(cls, in_file: str, values: Dict[str, Any]) -> str:
        if in_file == "":
            filename: str = read_latest_db_entry("TestWriteOutput", "outfile_name")
            in_file: str = f"{values['lute_config'].work_dir}/{filename}"
        return in_file


# smalldata_tools Parameters
############################
class SubmitSMDParameters(BaseBinaryParameters):
    """Parameters for running smalldata to produce reduced HDF5 files."""

    executable: str = Field("mpirun", description="MPI executable.", flag_type="")
    np: conint(gt=2, le=120) = Field(
        120, description="Number of processes", flag_type="-"
    )
    p_arg1: str = Field(
        "python", description="Executable to run with mpi (i.e. python).", flag_type=""
    )
    u: str = Field(
        "", description="Python option for unbuffered output.", flag_type="-"
    )
    m: str = Field(
        "mpi4py.run",
        description="Python option to execute a module's contents as __main__ module.",
        flag_type="-",
    )
    producer: str = Field(
        "", description="Path to the SmallData producer Python script.", flag_type=""
    )
    run: str = Field(
        os.environ.get("RUN_NUM", ""), description="DAQ Run Number.", flag_type="--"
    )
    experiment: str = Field(
        os.environ.get("EXPERIMENT", ""),
        description="LCLS Experiment Number.",
        flag_type="--",
    )
    stn: NonNegativeInt = Field(0, description="Hutch endstation.", flag_type="--")
    nevents: int = Field(
        int(1e9), description="Number of events to process.", flag_type="--"
    )
    directory: Optional[str] = Field(
        None,
        description="Optional output directory. If None, will be in ${EXP_FOLDER}/hdf5/smalldata.",
        flag_type="--",
    )
    gather_interval: PositiveInt = Field(
        25, description="Number of events to collect at a time.", flag_type="--"
    )
    norecorder: bool = Field(
        False, description="Whether to ignore recorder streams.", flag_type="--"
    )
    url: HttpUrl = Field(
        "https://pswww.slac.stanford.edu/ws-auth/lgbk",
        description="Base URL for eLog posting.",
        flag_type="--",
    )
    epicsAll: bool = Field(
        False,
        description="Whether to store all EPICS PVs. Use with care.",
        flag_type="--",
    )
    full: bool = Field(
        False,
        description="Whether to store all data. Use with EXTRA care.",
        flag_type="--",
    )
    fullSum: bool = Field(
        False,
        description="Whether to store sums for all area detector images.",
        flag_type="--",
    )
    default: bool = Field(
        False,
        description="Whether to store only the default minimal set of data.",
        flag_type="--",
    )
    image: bool = Field(
        False,
        description="Whether to save everything as images. Use with care.",
        flag_type="--",
    )
    tiff: bool = Field(
        False,
        description="Whether to save all images as a single TIFF. Use with EXTRA care.",
        flag_type="--",
    )
    centerpix: bool = Field(
        False,
        description="Whether to mask center pixels for Epix10k2M detectors.",
        flag_type="--",
    )
    postRuntable: bool = Field(
        False,
        description="Whether to post run tables. Also used as a trigger for summary jobs.",
        flag_type="--",
    )
    wait: bool = Field(
        False, description="Whether to wait for a file to appear.", flag_type="--"
    )
    xtcav: bool = Field(
        False,
        description="Whether to add XTCAV processing to the HDF5 generation.",
        flag_type="--",
    )
    noarch: bool = Field(
        False, description="Whether to not use archiver data.", flag_type="--"
    )

    lute_template_cfg: TemplateConfig = TemplateConfig(template_dir="", output_dir="")

    @validator("producer", always=True)
    def validate_producer_path(cls, producer: str) -> str:
        return producer

    @validator("lute_template_cfg", always=True)
    def use_producer(
        cls, lute_template_cfg: TemplateConfig, values: Dict[str, Any]
    ) -> TemplateConfig:
        if not lute_template_cfg.output_dir:
            lute_template_cfg.output_dir = values["producer"]
        return lute_template_cfg

    # detnames: ThirdPartyParameters = ThirdPartyParameters({})
    # epicsPV: ThirdPartyParameters = ThirdPartyParameters({})
    # ttCalib: ThirdPartyParameters = ThirdPartyParameters({})
    # aioParams: ThirdPartyParameters = ThirdPartyParameters({})
    # getROIs: ThirdPartyParameters = ThirdPartyParameters({})
    # getAzIntParams: ThirdPartyParameters = ThirdPartyParameters({})
    # getAzIntPyFAIParams: ThirdPartyParameters = ThirdPartyParameters({})
    # getPhotonsParams: ThirdPartyParameters = ThirdPartyParameters({})
    # getDropletParams: ThirdPartyParameters = ThirdPartyParameters({})
    # getDroplet2Photons: ThirdPartyParameters = ThirdPartyParameters({})
    # getSvdParams: ThirdPartyParameters = ThirdPartyParameters({})
    # getAutocorrParams: ThirdPartyParameters = ThirdPartyParameters({})


class FindOverlapXSSParameters(TaskParameters):
    """TaskParameter model for FindOverlapXSS Task.

    This Task determines spatial or temporal overlap between an optical pulse
    and the FEL pulse based on difference scattering (XSS) signal. This Task
    uses SmallData HDF5 files as a source.
    """

    class ExpConfig(BaseModel):
        det_name: str
        ipm_var: str
        scan_var: Union[str, List[str]]

    class Thresholds(BaseModel):
        min_Iscat: Union[int, float]
        min_ipm: Union[int, float]

    class AnalysisFlags(BaseModel):
        use_pyfai: bool = True
        use_asymls: bool = False

    exp_config: ExpConfig
    thresholds: Thresholds
    analysis_flags: AnalysisFlags


# Config IO
###########
def parse_config(task_name: str = "test", config_path: str = "") -> TaskParameters:
    """Parse a configuration file and validate the contents.

    Args:
        task_name (str): Name of the specific task that will be run.

        config_path (str): Path to the configuration file.

    Returns:
        params (TaskParameters): A TaskParameters object of validated
            task-specific parameters. Parameters are accessed with "dot"
            notation. E.g. `params.param1`.

    Raises:
        ValidationError: Raised if there are problems with the configuration
            file. Passed through from Pydantic.
    """
    task_config_name: str = f"{task_name}Parameters"

    with open(config_path, "r") as f:
        docs: Iterator[Dict[str, Any]] = yaml.load_all(stream=f, Loader=yaml.FullLoader)
        header: Dict[str, Any] = next(docs)
        config: Dict[str, Any] = next(docs)

    lute_config: Dict[str, AnalysisHeader] = {"lute_config": AnalysisHeader(**header)}
    try:
        task_config: Dict[str, Any] = dict(config[task_name])
        lute_config.update(task_config)
    except KeyError as err:
        warnings.warn(
            (
                f"{task_name} has no parameter definitions in YAML file."
                " Attempting default parameter initialization."
            )
        )
    parsed_parameters: TaskParameters = globals()[task_config_name](lute_config)

    return parsed_parameters
