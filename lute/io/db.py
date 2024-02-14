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

__all__ = ["write_cfg_to_db", "read_latest_db_entry"]
__author__ = "Gabriel Dorlhiac"

import logging
import sqlite3
from typing import List, Dict, Dict, Any, Tuple, Optional

from .config import TaskParameters
from ..tasks.task import TaskResult, TaskStatus
from ..execution.executor import AnalysisConfig

if __debug__:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

logger: logging.Logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """General LUTE database error."""

    ...


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
) -> Tuple[Dict[str, Any], Dict[str, str], Dict[str, Any], Dict[str, str],]:
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
        entries (Dict[str, Any]): Converted {name:value} dictionary for Task
            specific parameters.

        columns (Dict[str, str]): Converted {name:type} dictionary for Task
            specific parameters.

        gen_entries (Dict[str, Any]): General configuration entry dictionary.

        gen_columns (Dict[str, str]): General configuration type
            information dictionary.
    """
    gen_entry: Dict[str, Any]
    gen_columns: Dict[str, str]
    entry: Dict[str, Any]
    columns: Dict[str, str]
    gen_entry, gen_columns = _dict_to_flatdicts(params.lute_config.dict())
    del params.lute_config
    entry, columns = _dict_to_flatdicts(params.dict())

    return (
        entry,
        columns,
        gen_entry,
        gen_columns,
    )


def _result_to_entry_cols(
    result: TaskResult,
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Adapts the required fields from a TaskResult to be entered into a table.

    Args:
        entries (Dict[str, Any]): Converted {name:value} dictionary.

        columns (Dict[str, str]): Converted {name:type} dictionary.
    """
    entry: Dict[str, Any] = {
        "task_status": str(result.task_status).split(".")[1],
        "summary": result.summary,
        "payload": result.payload,
        "impl_schemas": result.impl_schemas,
    }
    columns: Dict[str, str] = {
        "task_status": "TEXT",
        "summary": "TEXT",
        "payload": "BLOB",
        "impl_schemas": "TEXT",
    }

    return entry, columns


def _cfg_to_exec_entry_cols(
    cfg: AnalysisConfig,
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Converts AnalysisConfig to be entered into a exec_cfg table.

    Args:
        entries (Dict[str, Any]): Converted {name:value} dictionary.

        columns (Dict[str, str]): Converted {name:type} dictionary.
    """
    entry: Dict[str, Any] = {
        "env": ";".join(f"{key}={value}" for key, value in cfg.task_env.items()),
        "poll_interval": cfg.poll_interval,
        "communicator_desc": ";".join(desc for desc in cfg.communicator_desc),
    }
    columns: Dict[str, str] = {
        "env": "TEXT",
        "poll_interval": "REAL",
        "communicator_desc": "TEXT",
    }

    return entry, columns


def _get_table_cols_sqlite(con: sqlite3.Connection, table_name: str) -> Dict[str, str]:
    """Retrieve the columns currently present in a table.

    Args:
        con (sqlite3.Connection): Database connection.

        table_name (str): The table's name.

    Returns:
        cols (Dict[str, str]): A dictionary of column names and types.
    """
    res: sqlite3.Cursor = con.execute(f"PRAGMA table_info({table_name})")
    # Retrieves: list = [col_id, col_name, col_type, -, default_val, -]
    table_info: List[Tuple[int, str, str, int, str, int]] = res.fetchall()

    cols: Dict[str, str] = {col[1]: col[2] for col in table_info}
    return cols


def _compare_cols(
    cols1: Dict[str, str], cols2: Dict[str, str]
) -> Optional[Dict[str, str]]:
    """Compare whether two sets of columns are identical.

    The comparison is unidirectional - This function tests for columns present
    in `cols2` which are not present in `cols1`, but NOT vice versa. Switch the
    order of the arguments in order to retrieve the other comparison.

    Args:
        cols1 (Dict[str, str]): Dictionary of first set of column names and
            types.

        cols2 (Dict[str, str]): Dictionary of second set of column names and
            types.

    Returns:
        diff (Dict[str, str] | None): Any columns present in `cols2` which
            are not present in `cols1`. If `cols2` has no entries which are
            not present in `cols1`, returns `None`.
    """
    diff: Dict[str, str] = {}

    for col_name in cols2.keys():
        if col_name not in cols1.keys():
            diff[col_name] = cols2[col_name]

    return diff if diff else None


def _make_task_table_sqlite(
    con: sqlite3.Connection, task_name: str, columns: Dict[str, str]
) -> bool:
    """Create  a sqlite Task table for LUTE's specification.

    Args:
        con (sqlite3.Connection): Database connection.

        task_name (str): The Task's name. This will be provided by the Task.
            In most cases this is the Python class' name.

        columns (Dict[str, str]): A dictionary of columns in the format of
            {COLNAME:TYPE}. These match the parameters of the Task and the Result
            fields of the Task. Additional more general columns are appended
            within this function. Other helper functions can be used for
            generating the dictionary from a TaskParameters object.

    Returns:
        success (bool): Whether the table was created correctly or not.
    """
    # Check existence explicitly because may need to modify...
    if _does_table_exist_sqlite(con, task_name):
        # Compare current columns vs new columns - the same Task can have
        # different number of parameters -> May need to adjust cols.
        current_cols: Dict[str, str] = _get_table_cols_sqlite(con, task_name)
        if diff := _compare_cols(current_cols, columns):
            for col in diff.items():
                sql: str = f"ALTER TABLE {task_name} ADD COLUMN {col[0]} {col[1]}"
                with con:
                    con.execute(sql)

    # Table does not yet exist -> Create it
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


def _make_shared_table_sqlite(
    con: sqlite3.Connection, table_name: str, columns: Dict[str, str]
) -> bool:
    """Create a general configuration table.

    Args:
        con (sqlite3.Connection): Database connection.

        table_name (str): Name of the table to create.

        columns (Dict[str, str]): A dictionary of columns in the format of
            {COLNAME:TYPE}.
    """
    col_str: str = ", ".join(f"{col[0]} {col[1]}" for col in columns)
    db_str: str = f"{table_name}(id INTEGER PRIMARY KEY AUTOINCREMENT, {col_str})"
    sql: str = f"CREATE TABLE IF NOT EXISTS {db_str}"
    with con:
        con.execute(sql)
    return _does_table_exist_sqlite(con, table_name)


def _add_task_entry_sqlite(
    con: sqlite3.Connection,
    task_name: str,
    entry: Dict[str, Any],
) -> None:
    """Add an entry to a task table.

    Args:
        con (sqlite3.Connection): Database connection.

        task_name (str): The Task's name. This will be provided by the Task.
            In most cases this is the Python class' name.

        entry (Dict[str, Any]): A dictionary of entries in the format of
            {COLUMN: ENTRY}. These are assumed to match the columns of the table.
    """
    placeholder_str: str = ", ".join("?" for x in range(len(entry)))
    keys: List[str] = []
    values: List[str] = []
    for key, value in entry.items():
        keys.append(f'"{key}"')
        values.append(value)
    with con:
        ins_str: str = "".join(f':"{x}", ' for x in entry.keys())[:-2]
        res = con.execute(
            f"INSERT INTO {task_name} ({','.join(keys)}) VALUES ({placeholder_str})",
            values,
        )


def _add_row_no_duplicate(
    con: sqlite3.Connection, table_name: str, entry: Dict[str, Any]
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

        entry (Dict[str, Any]): A dictionary of entries in the format of
            {COLUMN: ENTRY}. These are assumed to match the columns of the table.

    Returns:
        row_id (int): The row id of the newly added entry or the last entry
            which matches the provided values.
    """
    match_strs: List[str] = [f"{key} LIKE '{val}'" for key, val in entry.items()]
    total_match: str = " AND ".join(s for s in match_strs)

    res: sqlite3.Cursor
    with con:
        res = con.execute(f"SELECT id FROM {table_name} WHERE {total_match}")
        if ids := res.fetchall():
            logging.debug(
                f"_{table_name}_table_entry: Rows matching {total_match}: {ids}"
            )
            return ids[-1][0]
        ins_str: str = "".join(f":{x}, " for x in entry.keys())[:-2]
        res = con.execute(
            f"INSERT INTO {table_name} ({','.join(entry.keys())}) VALUES ({ins_str})",
            entry,
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
        task_entry,  # Dict[str, Any]
        task_columns,  # Dict[str, str]
        gen_entry,  # Dict[str, Any]
        gen_columns,  # Dict[str, str]
    ) = _params_to_entry_cols(cfg.task_parameters)
    x, y = _result_to_entry_cols(cfg.task_result)
    task_entry.update(x)
    task_columns.update(y)
    # --- Table Creation ---#
    if not _make_shared_table_sqlite(con, "gen_cfg", gen_columns):
        raise DatabaseError("Could not make general configuration table!")
    if not _make_shared_table_sqlite(con, "exec_cfg", exec_columns):
        raise DatabaseError("Could not make Executor configuration table!")
    if not _make_task_table_sqlite(con, task_name, task_columns):
        raise DatabaseError(f"Could not make Task table for: {task_name}!")

    # --- Row Addition ---#
    gen_id: int = _add_row_no_duplicate(con, "gen_cfg", gen_entry)
    exec_id: int = _add_row_no_duplicate(con, "exec_cfg", exec_entry)

    full_task_entry: Dict[str, Any] = {
        "gen_cfg_id": gen_id,
        "exec_cfg_id": exec_id,
    }
    full_task_entry.update(task_entry)
    # Prepare flag to indicate whether the task entry is valid or not
    # By default we say it is assuming proper completion
    valid_flag: int = 1 if cfg.task_result.task_status == TaskStatus.COMPLETED else 0
    full_task_entry.update({"valid_flag": valid_flag})

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


def _select_from_db(
    con: sqlite3.Connection, table_name: str, col_name: str, condition: Dict[str, str]
) -> Any:
    param, val = next(iter(condition.items()))
    subs: List[str] = [param, val]
    sql: str = f"SELECT {col_name} FROM {table_name} WHERE {subs[0]} = {subs[1]}"
    res: sqlite3.Cursor = con.execute(sql)
    entries = res.fetchall()
    return entries[-1]


def read_latest_db_entry(task_name: str, param: str) -> Any:
    """Read most recent value entered into the database for a Task parameter.

    (Will be updated for schema compliance as well as Task name.)

    Args:
        task_name (str): The name of the Task to check the database for.

        param (str): The parameter name for the Task that we want to retrieve.

    Returns:
        val (Any): The most recently entered value for `param` of `task_name`
            that can be found in the database.
    """
    con: sqlite3.Connection = sqlite3.Connection("lute.db")
    return _select_from_db(con, task_name, param, {"valid_flag": "1"})
