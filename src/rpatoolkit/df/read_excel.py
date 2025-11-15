import polars as pl
import logging
import re
from polars._typing import FileSource, SchemaDict, ExcelSpreadsheetEngine
from typing import Any, Sequence

log = logging.getLogger(__name__)


def read_excel(
    source: FileSource,
    *,
    sheet_id: int | None = None,
    sheet_name: str | None = None,
    table_name: str | None = None,
    engine: ExcelSpreadsheetEngine = "calamine",
    engine_options: dict[str, Any] | None = None,
    read_options: dict[str, Any] | None = None,
    has_header: bool = True,
    columns: Sequence[int] | Sequence[str] | str | None = None,
    schema_overrides: SchemaDict | None = None,
    infer_schema_length: int | None = 100,
    include_file_paths: str | None = None,
    drop_empty_rows: bool = False,
    drop_empty_cols: bool = False,
    raise_if_empty: bool = True,
    header_row: int | None = None,
    cast: dict[str, pl.DataType] | None = None,
    read_all_sheets: bool = False,
    lower_column_names: bool = True,
    clean_column_names: bool = False,
) -> pl.LazyFrame | dict[str, pl.LazyFrame]:
    """
    Reads an Excel file into a Polars LazyFrame.

    This function extends Polars' read_excel functionality by adding automatic
    column name cleaning and optional data type casting after cleaning the columns. It reads an excel file and returns a LazyFrame or a dictionary of LazyFrames if read_all_sheets is True.

    Parameters
    ----------
    source :
        Path to the Excel file or file-like object to read
    sheet_id :
        Sheet number to read (cannot be used with sheet_name)
    sheet_name :
        Sheet name to read (cannot be used with sheet_id)
    table_name :
        Name of a specific table to read.
    engine : {'calamine', 'openpyxl', 'xlsx2csv'}
        Library used to parse the spreadsheet file; defaults to "calamine".
    engine_options :
       Additional options passed to the underlying engine's primary parsing constructor
    read_options :
        Options passed to the underlying engine method that reads the sheet data.
    has_header :
        Whether the sheet has a header row
    columns :
        Columns to read from the sheet; if not specified, all columns are read
    schema_overrides :
        Support type specification or override of one or more columns.
    infer_schema_length : int, optional
        Number of rows to infer the schema from
    read_options :
        Dictionary of read options passed to polars.read_excel
    drop_empty_rows :
        Remove empty rows from the result
    drop_empty_cols :
        Remove empty columns from the result
    raise_if_empty :
        Raise an exception if the resulting DataFrame is empty
    cast : dict[str, pl.DataType], optional
        Dictionary mapping column names to desired data types for casting.
    read_all_sheets : bool, default=False
        Read all sheets in the Excel workbook.
    lower_column_names : bool, default=True
        Convert column names to lowercase
    clean_column_names : bool, default=False
        Clean column names by stripping punctuation

    Returns
    -------
    LazyFrame
        A Polars LazyFrame

    dict[str, LazyFrame]
        if reading multiple sheets using read_all_sheets=True, "{sheetname: LazyFrame, ...}" dict is returned

    Raises
    ------
    ValueError
        If both sheet_id and sheet_name are specified
        If sheet_id is 0

    Note:
    -----
    Column names are stripped and converted to lowercase

    Examples
    --------
    >>> df = read_excel("data.xlsx")
    >>> df = read_excel("data.xlsx", sheet_name="Sheet1")
    >>> df = read_excel("data.xlsx", cast={"date": pl.Date, "value": pl.Float64})
    """

    if sheet_id is not None and sheet_name is not None:
        raise ValueError("sheet_id and sheet_name cannot be both specified.")

    if sheet_id == 0:
        raise ValueError("sheet_id must start from 1.")

    if header_row:
        if read_options:
            read_options["header_row"] = header_row
        else:
            read_options = {
                "header_row": header_row,
            }

    if read_all_sheets:
        df = pl.read_excel(
            source=source,
            sheet_id=0,  # sheet_id=0 is used to read all sheets
            columns=columns,
            read_options=read_options,
            drop_empty_rows=drop_empty_rows,
            drop_empty_cols=drop_empty_cols,
            raise_if_empty=raise_if_empty,
        )
        df = _read_all_sheets(df, cast=cast)
        return df

    df = pl.read_excel(
        source=source,
        sheet_id=sheet_id,  # sheet_id=0 is used to read all sheets
        sheet_name=sheet_name,
        table_name=table_name,
        engine=engine,
        engine_options=engine_options,
        read_options=read_options,
        has_header=has_header,
        columns=columns,
        schema_overrides=schema_overrides,
        infer_schema_length=infer_schema_length,
        include_file_paths=include_file_paths,
        drop_empty_rows=drop_empty_rows,
        drop_empty_cols=drop_empty_cols,
        raise_if_empty=raise_if_empty,
    )
    if lower_column_names:
        df = _lower_column_names(df)

    if clean_column_names:
        df = _clean_column_names(df)

    df = _cast_columns(df, cast=cast)
    return df.lazy()


def _clean_column_names(df: pl.DataFrame) -> pl.DataFrame:
    df.columns = [_strip_punctuation(col) for col in df.columns]
    return df


def _lower_column_names(df: pl.DataFrame) -> pl.DataFrame:
    df.columns = [col.strip().lower() for col in df.columns]
    return df


def _strip_punctuation(text: str, replacement: str = "") -> str:
    """
    Strip punctuations from a string, particularly useful for cleaning table column headers.

    Args:
        text (str): The input string (typically a column header name)
        replacement (str, optional): Character to replace punctuation with. Defaults to '' (removes punctuation).

    Returns:
        str: String with punctuation stripped or replaced

    Examples:
        >>> strip_punctuation("First Name!")
        'First Name'
        >>> strip_punctuation("Last, Name")
        'Last Name'
        >>> strip_punctuation("Age?", replacement='_')
        'Age_'
    """
    if not isinstance(text, str):
        raise TypeError("Input must be a string")
    cleaned_text = re.sub(r"[^\w\s]", replacement, text)
    return cleaned_text.strip()


def _cast_columns(
    df: pl.DataFrame, cast: dict[str, pl.DataType] | None = None
) -> pl.DataFrame:
    if cast is not None:
        for col, dtype in cast.items():
            col = col.strip().lower()
            if col not in df.columns:
                log.warning(f"Column {col} not found in dataframe.")
                continue

            df = df.with_columns(pl.col(col).cast(dtype, strict=False))

    return df


def _read_all_sheets(
    df: dict[str, pl.DataFrame],
    lower_column_names: bool = True,
    clean_column_names: bool = False,
    cast: dict[str, pl.DataType] | None = None,
) -> dict[str, pl.LazyFrame]:
    result_dfs: dict[str, pl.LazyFrame] = {}

    for sheet_name, df in df.items():
        if lower_column_names:
            df = _lower_column_names(df)

        if clean_column_names:
            df = _clean_column_names(df)

        df = _cast_columns(df, cast=cast)
        result_dfs[sheet_name.lower()] = df.lazy()

    return result_dfs
