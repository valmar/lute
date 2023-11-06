"""
Machinary for the IO of configuration YAML files and their validation.

Classes
-------
TaskParameters(BaseModel)
    Base class for Task parameters.
FindOverlapXSSParameters(TaskParameters)
    Parameter model for the FindOverlapXSS Task.

Functions
---------
parse_config(taskname: str, config_path: str) -> TaskParameters
    Parse a configuration file and return a TaskParameters object of
    validated parameters for a specific Task. Raises an exception if the
    provided configuration does not match the expected model.

Exceptions
----------
ValidationError
    Error raised by pydantic during data validation.
"""

__all__ = ["parse_config"]
__author__ = "Gabriel Dorlhiac"

from abc import ABC
from typing import List, Dict, Iterator, Dict, Any, Union

import yaml
from pydantic import BaseModel, ValidationError


####################
class TaskParameters(BaseModel):
    ...


class FindOverlapXSSParameters(TaskParameters):
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

    Parameters
    ----------
    task_name : str
        Name of the specific task that will be run.
    config_path : str
        Path to the configuration file.

    Returns
    -------
    params : TaskParameters
        A TaskParameters object of validated task-specific parameters.
        Parameters are accessed with "dot" notation. E.g. `params.param1`.

    Raises
    ------
    ValidationError
        Raised if there are problems with the configuration file.
    """
    task_config_name: str = f"{task_name}Parameters"
    # target_config: TaskConfig = globals()[task_config_name]()

    with open(config_path, "r") as f:
        docs: Iterator[Dict[str, Any]] = yaml.load_all(stream=f, Loader=yaml.FullLoader)
        header: Dict[str, Any] = next(docs)
        config: Dict[str, Any] = next(docs)

    parsed_config: TaskParameters = globals()[task_config_name](**config[task_name])

    return parsed_config
