"""Tools for working with the LUTE parameter and configuration database.

The current implementation relies on a sqlite backend database. In the future
this may change - therefore relatively few high-level API function calls are
intended to be public. These abstract away the details of the database interface
and work exclusively on LUTE objects.

Functions:
    write_cfg_to_db(cfg: AnalysisConfig) -> None: Writes the configuration to
        the backend database.

Exceptions:
    DatabaseError: Generic exception raised for LUTE database errors.
"""

__all__ = ["write_cfg_to_db"]
__author__ = "Gabriel Dorlhiac"

import logging
import sqlite3
from typing import List, Dict, Dict, Any, Tuple

from .config import TaskParameters
from ..tasks.task import TaskResult, TaskStatus
from ..execution.executor import AnalysisConfig

if __debug__:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

logger: logging.Logger = logging.getLogger(__name__)


def _print_table_info(con: sqlite3.Connection, table_name: str) -> None:
    """Prints information about columns/entries of a Task table.

    This is primarily a debugging function.

    Args:
        con (sqlite3.Connection): Database connection.

        table_name (str): The table to check for.
    """
    res: sqlite3.Cursor = con.execute(f"PRAGMA table_info({table_name})")
    print(res.fetchall())


def _does_table_exist_sqlite(con: sqlite3.Connection, table_name: str) -> bool:
    """Check whether a table exists.

    Args:
        con (sqlite3.Connection): Database connection.

        table_name (str): The table to check for.

    Returns:
        does_exist (bool): Whether the table exists.
    """
    res: sqlite3.Cursor = con.execute(
        f"SELECT name FROM sqlite_master WHERE name='{table_name}'"
    )
    if res.fetchone() is None:
        return False
    else:
        return True


def _check_type(value: Any) -> str:
    """Return SQL type for a value."""
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "REAL"
    elif isinstance(value, str):
        return "TEXT"
    else:
        return "BLOB"


def _dict_to_flatdicts(
    d: Dict[str, Any], curr_key: str = ""
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Flattens a dictionary delimiting parameters with a '.'.

    Works recursively. Also returns the type of each flattened value.

    Args:
        d (Dict[str, Any]): Dictionary to flatten.

        curr_key (str): Current flattened key.

    Returns:
        flattened_params (Dict[str, Any]): Dictionary of flatkey:value pairs.

        flattened_types (Dict[str, str]): Dictionary of flatkey:type(value) pairs.
            Types are one of TEXT, INTEGER, REAL.
    """
    param_list: List[Tuple[str, Any]] = []
    type_list: List[Tuple[str, str]] = []
    for key, value in d.items():
        flat_key: str
        if curr_key == "":
            flat_key = key
        else:
            flat_key = f"{curr_key}.{key}"
        if isinstance(value, dict):
            x, y = _dict_to_flatdicts(value, curr_key=flat_key)
            param_list.extend(x.items())
            type_list.extend(y.items())
        else:
            param_list.append((flat_key, value))
            type_list.append((flat_key, _check_type(value)))

    return dict(param_list), dict(type_list)


def _params_to_entry_cols(
    params: TaskParameters,
) -> Tuple[
    List[Tuple[str, Any]],
    List[Tuple[str, str]],
    List[Tuple[str, Any]],
    List[Tuple[str, str]],
]:
    """Adapts a TaskParameters object to be entered into a table.

    Extracts the appropriate names and types from a TaskParameters object.
    Compound types (e.g. dicts) are recursively extracted and are given names
    where subparameters are delimited by ".". E.g. a parameter such as:
        my_param = {
            "a": 1,
            "b": 0.1,
        }
    will be converted into the following entries:
        ("my_param.a", "INTEGER"), ("my_param.b", "REAL").

    The `lute_config` analysis header is separated out and returned as a separate
    set of entries and columns. This particular field of the `TaskParameters`
    object contains shared configuration between `Task`s which is stored in a
    separated table.

    Args:
        params (TaskParameters): The TaskParameters object to convert to columns.

    Returns:
        entries (List[Tuple[str, Any]]): Converted (name, value) tuples for Task
            specific parameters.

        columns (List[Tuple[str, str]]): Converted (name, type) tuples for Task
            specific parameters.

        gen_entries (List[Tuple[str, Any]]): General configuration entry tuple.

        gen_columns (List[Tuple[str, str]]): General configuration type
            information.
    """
    gen_entry: Dict[str, Any]
    gen_columns: Dict[str, str]
    entry: Dict[str, Any]
    columns: Dict[str, str]
    gen_entry, gen_columns = _dict_to_flatdicts(params.lute_config.dict())
    del params.lute_config
    entry, columns = _dict_to_flatdicts(params.dict())

    return (
        list(entry.items()),
        list(columns.items()),
        list(gen_entry.items()),
        list(gen_columns.items()),
    )


def _result_to_entry_cols(
    result: TaskResult,
) -> Tuple[List[Tuple[str, Any]], List[Tuple[str, str]]]:
    """Adapts the required fields from a TaskResult to be entered into a table.

    Args:
        entries (List[Tuple(str, Any)]): Converted (name, value) tuples.

        columns (List[Tuple(str, str)]): Converted (name, type) tuples.
    """
    entry: List[Tuple[str, Any]] = [
        ("task_status", str(result.task_status).split(".")[1]),
        ("summary", result.summary),
        ("payload", result.payload),
        ("impl_schemas", result.impl_schemas),
    ]
    columns: List[Tuple[str, str]] = [
        ("task_status", "TEXT"),
        ("summary", "TEXT"),
        ("payload", "BLOB"),
        ("impl_schemas", "TEXT"),
    ]

    return entry, columns


def _cfg_to_exec_entry_cols(
    cfg: AnalysisConfig,
) -> Tuple[List[Tuple[str, Any]], List[Tuple[str, str]]]:
    """Converts AnalysisConfig to be entered into a exec_cfg table.

    Args:
        entries (List[Tuple(str, Any)]): Converted (name, value) tuples.

        columns (List[Tuple(str, str)]): Converted (name, type) tuples.
    """
    entry: List[Tuple[str, Any]] = [
        ("env", ";".join(f"{key}={value}" for key, value in cfg.task_env.items())),
        ("poll_interval", cfg.poll_interval),
        ("communicator_desc", ";".join(desc for desc in cfg.communicator_desc)),
    ]
    columns: List[Tuple[str, str]] = [
        ("env", "TEXT"),
        ("poll_interval", "REAL"),
        ("communicator_desc", "TEXT"),
    ]

    return entry, columns


def _make_task_table_sqlite(
    con: sqlite3.Connection, task_name: str, columns: List[Tuple[str, str]]
) -> bool:
    """Create  a sqlite Task table for LUTE's specification.

    Args:
        con (sqlite3.Connection): Database connection.

        task_name (str): The Task's name. This will be provided by the Task.
            In most cases this is the Python class' name.

        columns (List[Tuple[str,str]]): A list of columns in the format of
            (NAME, TYPE). These match the parameters of the Task and the Result
            fields of the Task. Additional more general columns are appended
            within this function. Other helper functions can be used for
            generating the columns list from a TaskParameters object.

    Returns:
        success (bool): Whether the table was created correctly or not.
    """
    # Need to escape column names using double quotes since they
    # may contain periods.
    col_str: str = ", ".join(f'"{col[0]}" {col[1]}' for col in columns)
    db_str: str = (
        f"{task_name}(id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, "
        f"gen_cfg_id INTEGER, exec_cfg_id INTEGER, {col_str}, "
        "valid_flag INTEGER)"
    )
    sql: str = f"CREATE TABLE IF NOT EXISTS {db_str}"
    with con:
        con.execute(sql)
    return _does_table_exist_sqlite(con, task_name)


def _make_general_table_sqlite(
    con: sqlite3.Connection, columns: List[Tuple[str, str]]
) -> bool:
    """Create a general configuration table.

    Args:
        con (sqlite3.Connection): Database connection.

        columns (List[Tuple[str,str]]): A list of columns in the format of
            (NAME, TYPE). These match the parameters of the AnalysisHeader.
    """
    col_str: str = ", ".join(f"{col[0]} {col[1]}" for col in columns)
    db_str: str = f"gen_cfg(id INTEGER PRIMARY KEY AUTOINCREMENT, {col_str})"
    sql: str = f"CREATE TABLE IF NOT EXISTS {db_str}"
    with con:
        con.execute(sql)
    return _does_table_exist_sqlite(con, "gen_cfg")


def _make_executor_table_sqlite(
    con: sqlite3.Connection, columns: List[Tuple[str, str]]
) -> bool:
    """Create a general configuration table.

    Args:
        con (sqlite3.Connection): Database connection.

        columns (List[Tuple[str,str]]): A list of columns in the format of
            (NAME, TYPE). These match the parameters of the AnalysisHeader.
    """
    col_str: str = ", ".join(f"{col[0]} {col[1]}" for col in columns)
    db_str: str = f"exec_cfg(id INTEGER PRIMARY KEY AUTOINCREMENT, {col_str})"
    sql: str = f"CREATE TABLE IF NOT EXISTS {db_str}"
    with con:
        con.execute(sql)
    return _does_table_exist_sqlite(con, "exec_cfg")


def _add_task_entry_sqlite(
    con: sqlite3.Connection,
    task_name: str,
    entry: List[Tuple[str, Any]],
) -> None:
    """Add an entry to a task table.

    Args:
        con (sqlite3.Connection): Database connection.

        task_name (str): The Task's name. This will be provided by the Task.
            In most cases this is the Python class' name.

        entry (List[Tuple[str, Any]]): A list of columns in the format of
            (NAME, VALUE). These match all required fields.
    """
    ent_dict: Dict[str, Any] = dict(entry)
    placeholder_str: str = ", ".join("?" for x in range(len(entry)))
    keys: List[str] = []
    values: List[str] = []
    for key, value in ent_dict.items():
        keys.append(f'"{key}"')  # Escape names w/ quotes since they may have "."
        values.append(value)
    with con:
        ins_str: str = "".join(f':"{x}", ' for x in ent_dict.keys())[:-2]
        res = con.execute(
            f"INSERT INTO {task_name} ({','.join(keys)}) VALUES ({placeholder_str})",
            values,
        )


def _add_row_no_duplicate(
    con: sqlite3.Connection, table_name: str, entry: List[Tuple[str, Any]]
) -> int:
    """Add a new row to a table with no duplicates.

    This function will check first to see if the entry exists in the table. If
    there is already a row with the provided information, its ID is returned.
    Otherwise a new row is added and the ID of the newly inserted row is
    returned.

    The tables for general configuration and Executor configuration assume that
    there are no duplicates as information is intended to be shared and linked
    to by multiple Tasks.

    This function ASSUMES the table EXISTS. Perform creation and necessary
    existence checks before using it.

    Args:
        con (sqlite3.Connection): Database connection.

        table_name (str): The table to add a new row with `entry` values to.

        entry (List[Tuple[str, Any]]): A list of columns in the format of
            (NAME, VALUE). These are assumed to match the columns of the table.

    Returns:
        row_id (int): The row id of the newly added entry or the last entry
            which matches the provided values.
    """
    ent_dict: Dict[str, Any] = dict(entry)

    match_strs: List[str] = [f"{key} LIKE '{val}'" for key, val in ent_dict.items()]
    total_match: str = " AND ".join(s for s in match_strs)

    res: sqlite3.Cursor
    with con:
        res = con.execute(f"SELECT id FROM {table_name} WHERE {total_match}")
        if ids := res.fetchall():
            logging.debug(
                f"_{table_name}_table_entry: Rows matching {total_match}: {ids}"
            )
            return ids[-1][0]
        ins_str: str = "".join(f":{x}, " for x in ent_dict.keys())[:-2]
        res = con.execute(
            f"INSERT INTO {table_name} ({','.join(ent_dict.keys())}) VALUES ({ins_str})",
            ent_dict,
        )
        res = con.execute(f"SELECT id FROM {table_name} WHERE {total_match}")
        new_id: int = res.fetchone()[-1]
        logging.debug(
            f"_{table_name}_table_entry: No matching rows - adding new row: {new_id}"
        )
    return new_id


def _make_all_tables_and_entries(con: sqlite3.Connection, cfg: AnalysisConfig) -> None:
    """Prepares all the tables required for a given Task and adds entries.

    A complete description of a Task run requires the Task table itself as well
    as the `Executor` table, and General Config table. The Task table has
    columns to link the proper entries of the other two tables.

    Args:
        con (sqlite3.Connection): Database connection.

        cfg (AnalysisConfig): Complete AnalysisConfig object received from an
            Executor.
    """
    exec_entry, exec_columns = _cfg_to_exec_entry_cols(cfg)
    task_name: str = cfg.task_result.task_name
    # All `Task`s have an AnalysisHeader, but this info can be shared so is
    # split into a different table
    (
        task_entry,  # List[Tuple(str, Any)]
        task_columns,  # List[Tuple(str, str)]
        gen_entry,  # List[Tuple(str, Any)]
        gen_columns,  # List[Tuple(str, str)]
    ) = _params_to_entry_cols(cfg.task_parameters)
    x, y = _result_to_entry_cols(cfg.task_result)
    task_entry.extend(x)
    task_columns.extend(y)
    # --- Table Creation ---#
    if not _make_general_table_sqlite(con, gen_columns):
        raise DatabaseError("Could not make general configuration table!")
    if not _make_executor_table_sqlite(con, exec_columns):
        raise DatabaseError("Could not make Executor configuration table!")
    if not _make_task_table_sqlite(con, task_name, task_columns):
        raise DatabaseError(f"Could not make Task table for: {task_name}!")

    # --- Row Addition ---#
    gen_id: int = _add_row_no_duplicate(con, "gen_cfg", gen_entry)
    exec_id: int = _add_row_no_duplicate(con, "exec_cfg", exec_entry)

    full_task_entry: List[Tuple[str, Any]] = [
        ("gen_cfg_id", gen_id),
        ("exec_cfg_id", exec_id),
    ]
    full_task_entry.extend(task_entry)
    # Prepare flag to indicate whether the task entry is valid or not
    # By default we say it is assuming proper completion
    valid_flag: int = 1 if cfg.task_result.task_status == TaskStatus.COMPLETED else 0
    full_task_entry.extend([("valid_flag", valid_flag)])

    _add_task_entry_sqlite(con, task_name, full_task_entry)


def write_cfg_to_db(cfg: AnalysisConfig) -> None:
    """Write an AnalysisConfig object to the database.

    The AnalysisConfig object is maintained by the Executor and contains all
    information necessary to fully describe a single `Task` execution. The
    contained fields are split across multiple tables within the database as
    some of the information can be shared across multiple Tasks. Refer to
    `docs/design/database.md` for more information on the database specification.
    """
    con: sqlite3.Connection = sqlite3.Connection("lute.db")
    _make_all_tables_and_entries(con, cfg)


class DatabaseError(Exception):
    """General LUTE database error."""

    ...
