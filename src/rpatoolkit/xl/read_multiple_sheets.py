from typing import Any

import polars as pl

from rpatoolkit.df import safe_schema_override
from rpatoolkit.utils import strip_punctuation
from rpatoolkit.xl.helpers import (
    FindHeaderRowOptions,
    get_sheet_names,
    locate_header_row,
)


def read_multiple_sheets(
    source: Any,
    *,
    sheet_names: list[str] | None = None,
    visible_sheets_only: bool = False,
    lower_column_names: bool = True,
    lower_sheet_names: bool = True,
    clean_column_names: bool = False,
    schema_overrides: dict[str, pl.DataType] | None = None,
    schema_override_strict: bool = False,
    find_header_row: bool = False,
    find_header_row_opts: dict[str, FindHeaderRowOptions] | None = None,
) -> dict[str, pl.DataFrame]:
    """
    Read multiple worksheets from an excel file.

    Parameters
    ----------
    source : Any
        Path or file-like object
    sheet_names : list[str] | None, optional
        List of sheet names to read, by default None
    visible_sheets_only : bool, optional
        Whether to read only the visible sheets, by default False
    lower_column_names : bool, optional
        Whether to lower column names, by default True
    lower_sheet_names : bool, optional
        Whether to lower the sheet names in the resulting dictionary key, by default True
    clean_column_names : bool, optional
        Remove punctuation from column names, by default False
    schema_overrides : dict[str, pl.DataType] | None, optional
        Dictionary mapping column names to desired polars data types. You can specify columns of multiple sheets in one single dictionary to override. Will try to override only if the column exist in the sheet, by default None
    schema_override_strict : bool, optional
       Whether to raise an error if casting fails, by default False, by default False
    visible_rows_only : bool, optional
        Whether to only read the visible/filtered rows. Uses openpyxl (slower), by default False

    Returns
    -------
    dict[str, pl.DataFrame]
        Dictionary mapping sheet names to their corresponding DataFrame
    """

    if visible_sheets_only:
        sheet_names = get_sheet_names(source, visible_only=True)
    else:
        sheet_names = get_sheet_names(source)

    all_df = _read_all_sheets_to_df(
        source,
        sheet_names=sheet_names,
        find_header_row=find_header_row,
        find_header_row_opts=find_header_row_opts,
    )

    result_df = _format_df(
        all_df,
        lower_column_names=lower_column_names,
        clean_column_names=clean_column_names,
        schema_overrides=schema_overrides,
        schema_override_strict=schema_override_strict,
        lower_sheet_names=lower_sheet_names,
    )
    return result_df


def _format_df(
    all_df: dict[str, pl.DataFrame],
    lower_column_names: bool = True,
    clean_column_names: bool = False,
    schema_overrides: dict[str, pl.DataType] | None = None,
    schema_override_strict: bool = False,
    lower_sheet_names: bool = True,
):
    result_df: dict[str, pl.DataFrame] = {}
    for sheet_name, df in all_df.items():
        if lower_column_names:
            df.columns = [col.strip().lower() for col in df.columns]

        if clean_column_names:
            df.columns = [strip_punctuation(col) for col in df.columns]

        if schema_overrides:
            df = safe_schema_override(
                df, schema_overrides=schema_overrides, strict=schema_override_strict
            )

        if lower_sheet_names:
            sheet_name = sheet_name.strip().lower()

        result_df[sheet_name] = df

    return result_df


def _read_all_sheets_to_df(
    source: Any,
    *,
    sheet_names: list[str],
    find_header_row: bool = False,
    find_header_row_opts: dict[str, FindHeaderRowOptions] | None = None,
) -> dict[str, pl.DataFrame]:
    if not find_header_row:
        return pl.read_excel(source, sheet_name=sheet_names)

    all_df: dict[str, pl.DataFrame] = {}
    for sheet in sheet_names:
        header_row = None
        if find_header_row:
            opts = find_header_row_opts.get(sheet, {}) if find_header_row_opts else {}
            header_row = locate_header_row(source, **opts)

        all_df[sheet] = pl.read_excel(
            source,
            sheet_name=sheet,
            read_options={"header_row": header_row} if header_row else None,
        )

    return all_df
