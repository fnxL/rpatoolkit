import logging
import polars as pl
from typing import overload
from rpatoolkit.utils import strip_punctuation

log = logging.getLogger(__name__)


@overload
def normalize_columns(
    df: pl.LazyFrame,
    mapping: dict[str, list[str] | str],
    remove_punctuation: bool = ...,
    lowercase_columns: bool = ...,
) -> pl.LazyFrame: ...


@overload
def normalize_columns(
    df: pl.DataFrame,
    mapping: dict[str, list[str] | str],
    remove_punctuation: bool = ...,
    lowercase_columns: bool = ...,
) -> pl.DataFrame: ...


def normalize_columns(
    df: pl.DataFrame | pl.LazyFrame,
    mapping: dict[str, list[str] | str],
    remove_punctuation: bool = True,
    lowercase_columns: bool = True,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Normalize and rename columns of a Polars DataFrame/LazyFrame based on a mapping.

    Args:
        df: Polars DataFrame or LazyFrame.
        mapping: dict where keys are final column names and values are either:
                 - a list of possible input column names
                 - a single possible input column name (string)
        remove_punctuation: Whether to strip punctuation from column names.
        lowercase_columns: Whether to lowercase all column names.

    Returns:
        A DataFrame/LazyFrame with standardized column names.
    """
    # Build a reverse mapping for O(1) lookup of actual_column_names to final_column_name
    reverse_mapping = {}
    for final_name, possible_names in mapping.items():
        if isinstance(possible_names, str):
            possible_names = [possible_names]

        for name in possible_names:
            key = name.lower()
            if key in reverse_mapping:
                raise ValueError(
                    f"Ambiguous mapping: '{name}' maps to both "
                    f"'{reverse_mapping[key]}' and '{final_name}'"
                )
            reverse_mapping[key] = final_name

    if isinstance(df, pl.LazyFrame):
        original_cols = df.collect_schema().names()
    else:
        original_cols = df.columns

    normalized_cols = original_cols.copy()
    if remove_punctuation:
        normalized_cols = [strip_punctuation(c.strip()) for c in normalized_cols]

    if lowercase_columns:
        normalized_cols = [c.strip().lower() for c in normalized_cols]

    rename_dict = {}
    used_final = set()

    for orig_col, norm_col in zip(original_cols, normalized_cols):
        norm_col = norm_col.strip().lower()
        if norm_col in reverse_mapping:
            final_name = reverse_mapping[norm_col]
            if final_name in used_final:
                raise ValueError(
                    f"Multiple columns map to the same final name '{final_name}'"
                )

            used_final.add(final_name)
            if orig_col != final_name:
                rename_dict[orig_col] = final_name

    if rename_dict:
        df = df.rename(rename_dict)

    return df


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
    selected_cols = [pl.col(col) for col in columns_order if col in df.columns]
    remaining_cols = [pl.col(col) for col in df.columns if col not in columns_order]
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
