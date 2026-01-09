from typing import Any

import polars as pl

from rpatoolkit.df import safe_schema_override
from rpatoolkit.utils import strip_punctuation
from rpatoolkit.xl.helpers import (
    FindHeaderRowOptions,
    get_sheet_names,
    locate_header_row,
    read_visible_rows,
)


def read_sheet(
    source: Any,
    *,
    sheet_name: str | None = None,
    find_header_row: bool = False,
    find_header_row_opts: FindHeaderRowOptions | None = None,
    header_row: int | None = None,
    first_visbile_sheet: bool = False,
    visible_rows_only: bool = False,
    lower_column_names: bool = True,
    clean_column_names: bool = False,
    schema_overrides: dict[str, pl.DataType] | None = None,
    schema_override_strict: bool = False,
    raise_if_empty: bool = True,
    drop_empty_rows: bool = True,
    drop_empty_cols: bool = True,
) -> pl.DataFrame:
    """
    Reads a single sheet from an excel file. If sheet_name is not provided, reads the first sheet in the workbook either visible or hidden.

    Parameters
    ----------
    source : Any
        Path or File-like object
    sheet_name : str | None, optional
        Name of the sheet to read, by default None. If None, reads the first sheet.
    find_header_row : bool, optional
        Whether to find the header row first before reading, by default False
    find_header_row_opts : FindHeaderRowOptions | None, optional
        Options for finding the header row, by default None
    header_row : int, optional
        Row number to use as header (0-indexed). This overrides find_header_row, by default None
    first_visbile_sheet : bool, optional
        Whether to read the first visible sheet. This skips the sheets that are hidden in the workbook and reads, by default False
    visible_rows_only : bool, optional
        Whether to only read the visible/filtered rows. Uses openpyxl (slower), by default False
    lower_column_names : bool, optional
        Convert column names to lowercase, by default True
    clean_column_names : bool, optional
        Clean column names by stripping punctuation, by default False
    schema_overrides : dict[str, pl.DataType] | None, optional
        Dictionary mapping column names to desired polars data types, by default None
    schema_override_strict : bool, optional
        Whether to raise an error if casting fails, by default False
    raise_if_empty : bool, optional
        Raise an exception if the resulting sheet is empty, by default True
    drop_empty_rows : bool, optional
        Remove empty rows from the sheet, by default False
    drop_empty_cols : bool, optional
        Remove empty columns from the sheet, by default False

    Note:
    -----
    Column names are stripped and converted to lowercase when lower_column_names=True
    """
    if find_header_row and header_row:
        # header_row overrides find_header_row, no need to find header row if header_row is specified
        find_header_row = False

    if find_header_row:
        header_row = locate_header_row(
            source,
            **find_header_row_opts if find_header_row_opts else {},
        )

    if first_visbile_sheet:
        visible_sheets = get_sheet_names(source, visible_only=True)
        if visible_sheets:
            sheet_name = visible_sheets[0]

    if visible_rows_only:
        df = read_visible_rows(
            source,
            sheet_name=sheet_name,
            header_row=header_row,
        )
    else:
        read_options = {"header_row": header_row} if header_row else None
        df = pl.read_excel(
            source,
            sheet_name=sheet_name,
            read_options=read_options,
            drop_empty_cols=drop_empty_cols,
            drop_empty_rows=drop_empty_rows,
        )

    if raise_if_empty and df.height == 0:
        raise ValueError(f"No rows found in the sheet: '{sheet_name or 'Default'}'")

    if lower_column_names:
        df.columns = [col.strip().lower() for col in df.columns]

    if clean_column_names:
        df.columns = [strip_punctuation(col) for col in df.columns]

    if schema_overrides:
        df = safe_schema_override(
            df, schema_overrides=schema_overrides, strict=schema_override_strict
        )

    return df
