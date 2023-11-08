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

from abc import ABC
from typing import List, Dict, Iterator, Dict, Any, Union

import yaml
from pydantic import BaseModel, BaseSettings, ValidationError


# Parameter models
##################
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

    parsed_config: TaskParameters = globals()[task_config_name](**config[task_name])

    return parsed_config
