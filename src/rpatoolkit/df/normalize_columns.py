import polars as pl

from rpatoolkit.utils import strip_punctuation


def normalize_columns(
    df: pl.DataFrame,
    mapping: dict[str, list[str] | str],
):
    """
    Normalize and rename columns of a polars dataframe based on column mapping

    Parameters
    ----------
    df : pl.DataFrame
        Polars dataframe
    mapping : dict[str, list[str]  |  str]
        dict where keys are standard column names and values are either
        - a list of possible column names
        - a single column name (string)

    Returns
    -------
    tuple[pl.DataFrame, dict[str, str]]
        A tuple containing the normalized dataframe and a dictionary mapping original column names to standardized column names to be used for denormalizing columns.

    Raises
    ------
    ValueError
        If different possible names in two different standard names, map to the same standard name.
    ValueError
        If multiple columns map to the same standard name.
    """

    if not mapping:
        return df

    # Build a reverse mapping for O(1) lookup of possible column names to standard column name
    reverse_lookup = {}
    for standard_name, possible_names in mapping.items():
        if isinstance(possible_names, str):
            possible_names = [possible_names]

        for name in possible_names:
            clean_key = strip_punctuation(name.strip().lower())

            if clean_key in reverse_lookup:
                raise ValueError(
                    f"Ambiguous mapping: '{name}' maps to both "
                    f"'{reverse_lookup[clean_key]}' and '{standard_name}'"
                )

            reverse_lookup[clean_key] = standard_name

    # Get original column names
    original_cols = df.columns

    rename_map = {}
    restore_map = {}

    used_final_names = set()

    for orig_col in original_cols:
        temp_col = strip_punctuation(orig_col.strip().lower())
        final_name = reverse_lookup.get(temp_col, temp_col)

        if final_name in used_final_names:
            # Handle Collision
            raise ValueError(
                f"Multiple columns map to the same final name '{final_name}'. Current col: {orig_col}"
            )

        used_final_names.add(final_name)

        if final_name != orig_col:
            rename_map[orig_col] = final_name

        restore_map[final_name] = orig_col

    if rename_map:
        df = df.rename(rename_map)

    return df, restore_map


def denormalize_columns(df: pl.DataFrame, restore_map: dict[str, str]) -> pl.DataFrame:
    """
    Renames columns back to their original state using the map generated during normalization.
    """
    current_cols = df.columns

    valid_rename_map = {k: v for k, v in restore_map.items() if k in current_cols}

    return df.rename(valid_rename_map)
