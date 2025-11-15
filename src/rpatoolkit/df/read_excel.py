import polars as pl
import logging
from polars._typing import FileSource, SchemaDict, ExcelSpreadsheetEngine
from typing import Any, Sequence

log = logging.getLogger(__name__)


def find_header_row(
    source: FileSource,
    sheet_id: int | None = None,
    sheet_name: str | None = None,
    max_rows: int = 200,
    expected_keywords: list[str] | None = None,
) -> int:
    """
    Find the header row in an Excel file by identifying the first row with maximum consecutive non-null values.

    This function is designed for Excel sheets where headers are not at the top row and the rows
    above the header row contain data. It identifies the most likely header row by scanning for
    the row with the highest number of consecutive non-null string values.

    Parameters
    ----------
    source : FileSource
        Path to the Excel file or file-like object to read
    sheet_id : int, optional
        Sheet number to read (cannot be used with sheet_name)
    sheet_name : str, optional
        Sheet name to read (cannot be used with sheet_id)
    max_rows : int, default=100
        Maximum number of rows to scan for header identification
    expected_keywords : list[str], optional
        List of keywords to look for in the header row. If a row contains all of these keywords, it is considered a header row.

    Returns
    -------
    int
        The zero-based index of the first row with maximum consecutive non-null values. If expected_keywords is provided, this is the first row with all expected keywords and maximum consecutive non-null values.

    Examples
    --------
    >>> header_row_index = find_header_row("data.xlsx")
    >>> df = read_excel("data.xlsx", header_row=header_row_index)

    Notes
    -----
    - If the first few rows are empty and the header is at the top of the data section,
      use `read_excel` directly instead of this function
    - The function considers consecutive non-null values from the beginning of each row
    - Only one of `sheet_id` or `sheet_name` can be specified
    """
    if sheet_id is not None and sheet_name is not None:
        raise ValueError("sheet_id and sheet_name cannot be both specified.")

    # Read first max_rows without assuming header row
    df = pl.read_excel(
        source,
        sheet_id=sheet_id,
        sheet_name=sheet_name,
        drop_empty_cols=False,
        drop_empty_rows=False,
        has_header=False,
    ).head(max_rows)

    max_consecutive = 0
    header_row_index = 0
    for i, row in enumerate(df.rows()):
        consecutive_count = 0
        # If expected keywords are provided, check if all of them are present in the row
        all_keywords_present = False
        if expected_keywords:
            row_values = [
                str(value).strip().lower() for value in row if value is not None
            ]
            all_keywords_present = all(
                keyword.lower() in row_values for keyword in expected_keywords
            )

        # Check for non-null consecutive values
        for value in row:
            if value is not None and str(value).strip() != "":
                consecutive_count += 1
            else:
                break

        if consecutive_count > max_consecutive:
            max_consecutive = consecutive_count
            header_row_index = i
            if expected_keywords and all_keywords_present:
                # This is the first row with all expected keywords, and highest consecutive non-null count, so its most likely the header row
                log.info(
                    "Found first header row with all expected keywords and maximum consecutive non-null values"
                )
                break

    log.info(
        f"Identified header row at index: {header_row_index} with {max_consecutive} consecutive non-null values"
    )
    return header_row_index


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
    **kwargs : Any
        Additional keyword arguments passed to polars.read_excel

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
    df = _lower_column_names(df)
    df = _cast_columns(df, cast=cast)
    return df.lazy()


def _lower_column_names(df: pl.DataFrame) -> pl.DataFrame:
    df.columns = [col.strip().lower() for col in df.columns]
    return df


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
    cast: dict[str, pl.DataType] | None = None,
) -> dict[str, pl.LazyFrame]:
    result_dfs: dict[str, pl.LazyFrame] = {}

    for sheet_name, df in df.items():
        df = _lower_column_names(df)
        df = _cast_columns(df, cast=cast)
        result_dfs[sheet_name.lower()] = df.lazy()

    return result_dfs
