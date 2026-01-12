import logging

import polars as pl

log = logging.getLogger(__name__)


def reorder_columns(
    df: pl.DataFrame | pl.LazyFrame, columns_order: list[str]
) -> pl.DataFrame | pl.LazyFrame:
    """
    Reorder columns of a Polars DataFrame or LazyFrame.

    Args:
        df (pl.DataFrame | pl.LazyFrame): The input DataFrame or LazyFrame.
        columns_order (list[str]): A list specifying the desired order of columns or subset of columns that you want to be ordered.

    Returns:
        pl.DataFrame | pl.LazyFrame: The DataFrame or LazyFrame with reordered columns.

    Example:
        >>> import polars as pl
        >>> df = pl.DataFrame({
        ...     "A": [1, 2, 3],
        ...     "B": [4, 5, 6],
        ...     "C": [7, 8, 9]
        ... })
        >>> reordered_df = reorder_columns(df, ["C", "A"])
        >>> print(reordered_df)
        shape: (3, 3)
        ┌─────┬─────┬─────┐
        │ C   ┆ A   ┆ B   │
        │ --- ┆ --- ┆ --- │
        │ i64 ┆ i64 ┆ i64 │
        ╞═════╪═════╡
        │ 7   ┆ 1   ┆ 4   │
        │ 8   ┆ 2   ┆ 5   │
        │ 9   ┆ 3   ┆ 6   │
        └─────┴─────┘

    """
    # Select the specified columns in the desired order, then append any remaining columns
    if isinstance(df, pl.LazyFrame):
        df_cols = df.collect_schema().names()
    else:
        df_cols = df.columns

    selected_cols = [pl.col(col) for col in columns_order if col in df_cols]
    remaining_cols = [pl.col(col) for col in df_cols if col not in columns_order]
    return df.select(selected_cols + remaining_cols)


def get_missing_columns(
    df: pl.DataFrame | pl.LazyFrame, required_columns: list[str]
) -> list[str]:
    """
    Check if a Polars DataFrame or LazyFrame contains all required columns and return a list of missing columns.

    Args:
        df (pl.DataFrame | pl.LazyFrame): The input DataFrame or LazyFrame.
        required_columns (list[str]): A list of required column names.

    Returns:
        list
            A list of missing columns from the required_columns list.

    Example:
        >>> import polars as pl
        >>> df = pl.DataFrame({
        ...     "A": [1, 2, 3],
        ...     "B": [4, 5, 6],
        ...     "C": [7, 8, 9]
        ... })
        >>> missing_columns = get_missing_columns(df, ["C", "E"])
        >>> print(missing_columns)
        ['E']
    """

    if isinstance(df, pl.LazyFrame):
        available_columns = [col.lower() for col in df.collect_schema().names()]
    else:
        available_columns = [col.lower() for col in df.columns]

    missing_cols = [
        col for col in required_columns if col.lower() not in available_columns
    ]
    return missing_cols


def safe_schema_override(
    df: pl.DataFrame,
    *,
    schema_overrides: dict[str, pl.DataType],
    strict: bool = False,
) -> pl.DataFrame:
    if not schema_overrides:
        return df

    # Create lower column name to actual column name map
    df_col_map = {}
    for col in df.columns:
        lower_col = col.strip().lower()
        df_col_map[lower_col] = col

    # Create valid schema overrides
    valid_schema_overrides = {}
    for col, dtype in schema_overrides.items():
        lower_col = col.strip().lower()
        if lower_col in df_col_map:
            orig_col = df_col_map[lower_col]
            valid_schema_overrides[orig_col] = dtype

    return df.cast(valid_schema_overrides, strict=strict)
