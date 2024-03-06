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
- The model must have one field specified called `executable`. The presence of this field indicates that the `Task` is a third-party `Task`. This allows all third-party `Task`s to be defined exclusively by their parameters model. A single `BinaryTask` class handles execution of **all** third-party `Task`s.

All models are stored in `lute/io/models`. For any given `Task`, a new model can be added to an existing module contained in this directory or to a new module. If creating a new module, make sure to add an import statement to `lute.io.models.__init__`.

When specifying parameters the default behaviour is to provide a one-to-one correspondance between the Python attribute specified in the parameter model, and the parameter specified on the command-line. Single-letter attributes are assumed to be passed using `-`, e.g. `n` will be passed as `-n` when the executable is launched. Longer attributes are passed using `--`, e.g. by default a model attribute named `my_arg` will be passed on the command-line as `--my_arg`. Positional arguments are specified using `p_argX` where `X` is a number. All parameters are passed in the order that they are specified in the model.

However, because the number of possible command-line combinations is large, relying on the default behaviour above is **NOT recommended**. It is provided solely as a fallback. Instead, there are a number of configuration knobs which can be tuned to achieve the desired behaviour. The two main mechanisms for controlling behaviour are specification of model-wide configuration under the `Config` class within the model's definition, and parameter-by-parameter configuration using field attributes. For the latter, we define all parameters as `Field` objects. This allows parameters to have their own attributes, which are parsed by LUTE's task-layer. Given this, the preferred starting template for a `TaskParameters` model is the following - we assume we are integrating a new `Task` called `RunTask`:

```py

from pydantic import Field, validator # Also include any pydantic type specifications

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


| **Config Parameter** | **Meaning**                                                       | **Default Value** |
|:--------------------:|:-----------------------------------------------------------------:|:-----------------:|
| `short_flags_use_eq` | Use equals sign instead of space for arguments of `-` parameters. | `False`           |
| `long_flags_use_eq`  | Use equals sign instead of space for arguments of `-` parameters. |                   |


| **Field Parameter** | **Meaning**                                                                       | **Default Value** | **Example**                                  |
|:-------------------:|:---------------------------------------------------------------------------------:|:-----------------:|:--------------------------------------------:|
| `flag_type`         | Specify the type of flag for passing this argument. One of `"-"`, `"--"`, or `""` | N/A               | `p_arg1 = Field(..., flag_type="")`          |
| `rename_param`      | Change the name of the parameter as passed on the command-line.                   | N/A               | `my_arg = Field(..., rename_param="my-arg")` |
|                     |                                                                                   |                   |                                              |

#### FAQ
1. How can I specify a default value which depends on another parameter?
2. My new `Task` depends on the output of a previous `Task`, how can I specify this dependency?


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
