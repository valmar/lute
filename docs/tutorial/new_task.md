# Integrating a New `Task`

`Task`s can be broadly categorized into two types:
- "First-party" - where the analysis or executed code is maintained within this library.
- "Third-party" - where the analysis, code, or program is maintained elsewhere and is simply called by a wrapping `Task`.

Creating a new `Task` of either type generally involves the same steps, although for first-party `Task`s, the analysis code must of course also be written. Due to this difference, as well as additional considerations for parameter handling when dealing with "third-party" `Task`s, the "first-party" and "third-party" `Task` integration cases will be considered separately.

## Creating a "Third-party" `Task`

There are two required steps for third-party `Task` integration, and one additional step which is optional, and may not be applicable to all possible third-party `Task`s. Generally, `Task` integration requires:
1. Defining a `TaskParameters` (pydantic) model which fully parameterizes the `Task`. This involves specifying a path to a binary, and all the required command-line arguments to run the binary.
2. Creating a **managed `Task`** by specifying an `Executor` for the new third-party `Task`. At this stage, any additional environment variables can be added which are required for the execution environment.
3. **(Optional/Maybe applicable)** Create a template for a third-party configuration file. If the new `Task` has its own configuration file, specifying a template will allow that file to be parameterized from the singular LUTE yaml configuration file. A couple of minor additions to the `pydantic` model specified in 1. are required to support template usage.

Each of these stages will be discussed in detail below. The vast majority of the work is completed in step 1.

### Specifying a `TaskParameters` Model for your `Task`.

A brief overview of parameters objects will be provided below. The following information goes into detail only about specifics related to LUTE configuration. An in depth description of pydantic is beyond the scope of this tutorial; please refer to the [official documentation](https://docs.pydantic.dev/1.10/) for more information. Please note that due to environment constraints pydantic is currently pinned to version 1.10! Make sure to read the appropriate documentation for this version as many things are different compared to the newer releases. At the end this document there will be an example highlighting some supported behaviour as well as a FAQ to address some common integration considerations.

**`Task`s and `TaskParameter`s**

All `Task`s have a corresponding `TaskParameters` object. These objects are linked **exclusively** by a named relationship. For a `Task` named `MyBinaryTask`, the parameters object **must** be named `MyBinaryTaskParameters`. For third-party `Task`s there are a number of additional requirements:
- The model must inherit from a base class called `BaseBinaryParameters`.
- The model must have one field specified called `executable`. The presence of this field indicates that the `Task` is a third-party `Task` and the specified executable must be called. This allows all third-party `Task`s to be defined exclusively by their parameters model. A single `BinaryTask` class handles execution of **all** third-party `Task`s.

All models are stored in `lute/io/models`. For any given `Task`, a new model can be added to an existing module contained in this directory or to a new module. If creating a new module, make sure to add an import statement to `lute.io.models.__init__`.

**Defining `TaskParameter`s**

When specifying parameters the default behaviour is to provide a one-to-one correspondance between the Python attribute specified in the parameter model, and the parameter specified on the command-line. Single-letter attributes are assumed to be passed using `-`, e.g. `n` will be passed as `-n` when the executable is launched. Longer attributes are passed using `--`, e.g. by default a model attribute named `my_arg` will be passed on the command-line as `--my_arg`. Positional arguments are specified using `p_argX` where `X` is a number. All parameters are passed in the order that they are specified in the model.

However, because the number of possible command-line combinations is large, relying on the default behaviour above is **NOT recommended**. It is provided solely as a fallback. Instead, there are a number of configuration knobs which can be tuned to achieve the desired behaviour. The two main mechanisms for controlling behaviour are specification of model-wide configuration under the `Config` class within the model's definition, and parameter-by-parameter configuration using field attributes. For the latter, we define all parameters as `Field` objects. This allows parameters to have their own attributes, which are parsed by LUTE's task-layer. Given this, the preferred starting template for a `TaskParameters` model is the following - we assume we are integrating a new `Task` called `RunTask`:

```py

from pydantic import Field, validator
# Also include any pydantic type specifications - Pydantic has many custom
# validation types already, e.g. types for constrained numberic values, URL handling, etc.

from .base import BaseBinaryParameters

# Change class name as necessary
class RunTaskParameters(BaseBinaryParameters):
    """Parameters for RunTask..."""

    class Config(BaseBinaryParameters.Config): # MUST be exactly as written here.
        ...
        # Model-wide configuration will go here

    executable: str = Field("/path/to/executable", description="...")
    ...
    # Additional params.
    # param1: param1Type = Field("default", description="", ...)
```

Under the class definition for `Config` in the model, we can modify global options for all the parameters. Currently, the available configuration options are:

| **Config Parameter** | **Meaning**                                                       | **Default Value** |
|:--------------------:|:-----------------------------------------------------------------:|:-----------------:|
| `short_flags_use_eq` | Use equals sign instead of space for arguments of `-` parameters. | `False`           |
| `long_flags_use_eq`  | Use equals sign instead of space for arguments of `-` parameters. | `False`           |
|                      |                                                                   |                   |

These configuration options modify how the parameter models are parsed and passed along on the command-line. The default behaviour is that parameters are assumed to be passed as `-p arg` and `--param arg`. Setting the above options to `True` will mean that all parameters are instead passed as `-p=arg` and `--param=arg`.

In addition to the global configuration options there are a couple of ways to specify individual parameters. The following `Field` attributes are used when parsing the model:

| **Field Attribute** | **Meaning**                                                                       | **Default Value** | **Example**                                       |
|:-------------------:|:---------------------------------------------------------------------------------:|:-----------------:|:-------------------------------------------------:|
| `flag_type`         | Specify the type of flag for passing this argument. One of `"-"`, `"--"`, or `""` | N/A               | `p_arg1 = Field(..., flag_type="")`               |
| `rename_param`      | Change the name of the parameter as passed on the command-line.                   | N/A               | `my_arg = Field(..., rename_param="my-arg")`      |
| `description`       | Documentation of the parameter's usage or purpose.                                | N/A               | `arg = Field(..., description="Argument for...")` |
|                     |                                                                                   |                   |                                                   |

The `flag_type` attribute allows us to specify whether the parameter corresponds to a positional (`""`) command line argument, requires a single hyphen (`"-"`), or a double hyphen (`"--"`). By default, the parameter name is passed as-is on the command-line. However, command-line arguments can have characters which would not be valid in Python variable names. In particular, hyphens are frequently used. To handle this case, the `rename_param` attribute can be used to specify an alternative spelling of the parameter when it is passed on the command-line. This also allows for using more descriptive variable names internally than those used on the command-line. A `description` can also be provided for each Field to document the usage and purpose of that particular parameter.

As an example, we can again consider defining a model for a `RunTask` `Task`. Consider an executable which would normally be called from the command-line as follows:
```bash
/sdf/group/lcls/ds/tools/runtask -n <nthreads> --method=<algorithm> -p <algo_param> [--debug]
```

A model specification for this `Task` may look like:
```py
class RunTaskParameters(BaseBinaryParameters):
    """Parameters for the runtask binary."

    class Config(BaseBinaryParameters.Config):
        long_flags_use_eq: bool = True  # For the --method parameter

    # Prefer using full/absolute paths where possible.
    # No flag_type needed for this field
    executable: str = Field(
        "/sdf/group/lcls/ds/tools/runtask", description="Runtask Binary v1.0"
    )

    # We can provide a more descriptive name for -n
    # Let's assume it's a number of threads, or processes, etc.
    num_threads: int = Field(
        1, description="Number of concurrent threads.", flag_type="-", rename_param="n"
    )

    # In this case we will use the Python variable name directly when passing
    # the parameter on the command-line
    method: str = Field("algo1", description="Algorithm to use.", flag_type="--")

    # For an actual parameter we would probably have a better name. Lets assume
    # This parameter (-p) modifies the behaviour of the method above.
    method_param1: int = Field(
        3, description="Modify method performance.", flag_type="-", rename_param="p"
    )

    # Boolean flags are only passed when True! `--debug` is an optional parameter
    # which is not followed by any arguments.
    debug: bool = Field(
        False, description="Whether to run in debug mode.", flag_type="--"
    )
```

**Additional Comments**
1. Model parameters of type `bool` are not passed with an argument and are only passed when `True`. This is a common use-case for boolean flags which enable things like test or debug modes, verbosity or reporting features. E.g. `--debug`, `--test`, `--verbose`, etc.
  - If you need to pass the literal words `"True"` or `"False"`, use a parameter of type `str`.
2. You can use `pydantic` types to constrain parameters beyond the basic Python types. E.g. `conint` can be used to define lower and upper bounds for an integer. There are also types for common categories, positive/negative numbers, paths, URLs, IP addresses, etc.
  - Even more custom behaviour can be achieved with `validator`s (see below).
3. All `TaskParameters` objects and its subclasses have access to a `lute_config` parameter, which is of type `lute.io.models.base.AnalysisHeader`. This special parameter is ignored when constructing the call for a binary task, but it provides access to shared/common parameters between tasks. For example, the following parameters are available through the `lute_config` object, and may be of use when constructing validators. All fields can be accessed with `.` notation. E.g. `lute_config.experiment`.
  - `title`: A user provided title/description of the analysis.
  - `experiment`: The current experiment name
  - `run`: The current acquisition run number
  - `date`: The date of the experiment or the analysis.
  - `lute_version`: The version of the software you are running.
  - `task_timeout`: How long a `Task` can run before it is killed.
  - `work_dir`: The main working directory for LUTE. Files and the database are created relative to this directory.

**Validators**
Pydantic uses `validators` to determine whether a value for a specific field is appropriate. There are default validators for all the standard library types and the types specified within the pydantic package; however, it is straightforward to define custom ones as well. In the template code-snippet above we imported the `validator` decorator. To create our own validator we define a method (with any name) with the following prototype, and decorate it with the `validator` decorator:
```py
@validator("name_of_field_to_decorate")
def my_custom_validator(cls, field: Any, values: Dict[str, Any]) -> Any: ...
```
In this snippet, the `field` variable corresponds to the value for the specific field we want to validate. `values` is a dictionary of fields and their values which have been parsed prior to the current field. This means you can validate the value of a parameter based on the values provided for other parameters. Since pydantic always validates the fields in the order they are defined in the model, fields dependent on other fields should come later in the definition.

For example, consider the `method_param1` field defined above for `RunTask`. We can provide a custom validator which changes the default value for this field depending on what type of algorithm is specified for the `--method` option. We will also constrain the options for `method` to two specific strings.

```py
from pydantic import Field, validator, ValidationError
class RunTaskParameters(BaseBinaryParameters):
    """Parameters for the runtask binary."""

    # [...]

    # In this case we will use the Python variable name directly when passing
    # the parameter on the command-line
    method: str = Field("algo1", description="Algorithm to use.", flag_type="--")

    # For an actual parameter we would probably have a better name. Lets assume
    # This parameter (-p) modifies the behaviour of the method above.
    method_param1: Optional[int] = Field(
        description="Modify method performance.", flag_type="-", rename_param="p"
    )

    # We will only allow method to take on one of two values
    @validator("method")
    def validate_method(cls, method: str, values: Dict[str, Any]) -> str:
        """Method validator: --method can be algo1 or algo2."""

        valid_methods: List[str] = ["algo1", "algo2"]
        if method not in valid_methods:
            raise ValueError("method must be algo1 or algo2")
        return method

    # Lets change the default value of `method_param1` depending on `method`
    # NOTE: We didn't provide a default value to the Field above and made it
    # optional. We can use this to test whether someone is purposefully
    # overriding the value of it, and if not, set the default ourselves.
    # We set `always=True` since pydantic will normally not use the validator
    # if the default is not changed
    @validator("method_param1", always=True)
    def validate_method_param1(cls, param1: Optional[int], values: Dict[str, Any]) -> int:
        """method param1 validator"""

        # If someone actively defined it, lets just return that value
        # We could instead do some additional validation to make sure that the
        # value they provided is valid...
        if param1 is not None:
            return param1

        # method_param1 comes after method, so this will be defined, or an error
        # would have been raised.
        method: str = values['method']
        if method == "algo1":
            return 3
        elif method == "algo2":
            return 5
```

#### FAQ
1. How can I specify a default value which depends on another parameter?

Use a custom validator. The example above shows how to do this. The parameter that depends on another parameter must come LATER in the model defintion than the independent parameter.

2. My new `Task` depends on the output of a previous `Task`, how can I specify this dependency?
Parameters used to run a `Task` are recorded in a database for every `Task`. It is also recorded whether or not the execution of that specific parameter set was successful. A utility function is provided to access the most recent values from the database for a specific parameter of a specific `Task`. It can also be used to specify whether unsuccessful `Task`s should be included in the query. This utility can be used within a validator to specify dependencies. For example, suppose the input of `RunTask2` (parameter `input`) depends on the output location of `RunTask1` (parameter `outfile`). A validator of the following type can be used to retrieve the output file and make it the default value of the input parameter.

```py
from pydantic import Field, validator

from .base import BaseBinaryParameters
from ..db import read_latest_db_entry

class RunTask2Parameters(BaseBinaryParameters):
    input: str = Field("", description="Input file.", flag_type="--")

    @validator("input")
    def validate_input(cls, input: str, values: Dict[str, Any]) -> str:
        if input == "":
            task1_out: Optional[str] = read_latest_db_entry(
                f"{values['lute_config'].work_dir}",  # Working directory. We search for the database here.
                "RunTask1",                           # Name of Task we want to look up
                "outfile",                            # Name of parameter of the Task
                valid_only=True,                      # We only want valid output files.
            )
            # read_latest_db_entry returns None if nothing is found
            if task1_out is not None:
                return task1_out
        return input
```

There are more examples of this pattern spread throughout the various `Task` models.

## Creating a "First-Party" `Task`
### Specifying a `TaskParameters` Model for your `Task`.
Parameter models have a format that must be followed for "Third-Party" `Task`s, but "First-Party" `Task`s have a little more liberty in how parameters are dealt with, since the `Task` will do all the parsing itself.

To create a model, the basic steps are:
1. If necessary, create a new module (e.g. `new_task_category.py`) under `lute.io.models`, or find an appropriate pre-existing module in that directory.
  - An `import` statement must be added to `lute.io.models._init_` if a new module is created, so it can be found.
  - If defining the model in a pre-existing module, make sure to modify the `__all__` statement to include it.
2. Create a new model that inherits from `TaskParameters`. You can look at `lute.models.io.tests.TestReadOutputParameters` for an example. **The model must be named** `<YourTaskName>Parameters`
  - You should include **all** relevant parameters here, including input file, output file, and any potentially adjustable parameters. These parameters **must** be included even if there are some implicit dependencies between `Task`s and it would make sense for the parameter to be auto-populated based on some other output. Creating this dependency is done with validators (see step 3.). All parameters should be overridable, and all `Task`s should be fully-independently configurable, based solely on their model and the configuration YAML.
  - To follow the preferred format, parameters should be defined as: `param_name: type = Field([default value], description="This parameter does X.")`
3. Use validators to do more complex things for your parameters, including populating default values dynamically:
  - E.g. create default values that depend on other parameters in the model - see for example: [SubmitSMDParameters](https://github.com/slac-lcls/lute/blob/57f2a0889ec9603e3b8642f485c27df7d1f6e96f/lute/io/models/smd.py#L139).
  - E.g. create default values that depend on other `Task`s by reading from the database - see for example: [TestReadOutputParameters](https://github.com/slac-lcls/lute/blob/57f2a0889ec9603e3b8642f485c27df7d1f6e96f/lute/io/models/tests.py#L75).
4. The model will have access to some general configuration values by inheriting from `TaskParameters`. These parameters are all stored in `lute_config` which is an instance of `AnalysisHeader` ([defined here](https://github.com/slac-lcls/lute/blob/57f2a0889ec9603e3b8642f485c27df7d1f6e96f/lute/io/models/base.py#L42)).
  - For example, the experiment and run number can be obtained from this object and a validator could use these values to define the default input file for the `Task`.
