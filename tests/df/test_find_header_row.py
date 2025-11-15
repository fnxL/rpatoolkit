import polars as pl
import tempfile
import os
from rpatoolkit.df import find_header_row


def test_find_header_row_with_proper_headers():
    """Test finding header row when headers are at the beginning"""
    # Create a test dataframe with headers at row 0
    df = pl.DataFrame(
        {
            "Name": ["John", "Jane", "Bob"],
            "Age": [25, 30, 35],
            "City": ["NYC", "LA", "Chicago"],
        }
    )

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        df.write_excel(tmp.name)
        result = find_header_row(tmp.name)
        assert result == 0

    os.unlink(tmp.name)


def test_find_header_row_with_offset_headers():
    df = pl.DataFrame(
        {
            "Name": ["Non Null", None, "Name", "Jane", "Bob"],
            "Age": [None, None, "Age", 30, 35],
            "City": [None, None, "City", "LA", "Chicago"],
        },
        strict=False,
    )
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        df.write_excel(tmp.name, include_header=False, position=(2, 0))
        # Test finding header row (should be 2)
        result = find_header_row(tmp.name)
        assert result == 2

    os.unlink(tmp.name)


def test_find_header_row_with_expected_keywords():
    from pathlib import Path

    if not Path("tests/data/header_row_offset_2.xlsx").exists():
        return

    result = find_header_row(
        "tests/data/header_row_offset_2.xlsx",
        expected_keywords=["po number"],
    )
    assert result == 2
