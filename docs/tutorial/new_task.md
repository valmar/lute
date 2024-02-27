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
