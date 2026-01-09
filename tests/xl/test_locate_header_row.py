import os
import tempfile

import polars as pl

from rpatoolkit.xl import locate_header_row


def test_locate_header_row_at_beginning():
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
        result = locate_header_row(tmp.name)
        assert result == 0

    os.unlink(tmp.name)


def test_locate_header_row_with_offset_headers():
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
        result = locate_header_row(tmp.name)
        assert result == 4

    os.unlink(tmp.name)


def test_locate_header_row_with_offset_headers_and_blank_row_at_top():
    df = pl.DataFrame(
        {
            "Name": [None, None, None, "Non Null", None, "Name", "Jane", "Bob"],
            "Age": [None, None, None, None, None, "Age", 30, 35],
            "City": [None, None, None, None, None, "City", "LA", "Chicago"],
        },
        strict=False,
    )
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        df.write_excel(tmp.name, include_header=False)
        result = locate_header_row(tmp.name)
        assert result == 5

    os.unlink(tmp.name)


def test_locate_header_row_with_expected_keywords():
    df = pl.DataFrame(
        {
            "Name": [None, None, None, "Non Null", None, "Name", "Jane", "Bob"],
            "Age": [None, None, None, None, None, "Age", 30, 35],
            "City": [None, None, None, None, None, "City", "LA", "Chicago"],
        },
        strict=False,
    )
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        df.write_excel(tmp.name, include_header=False)
        result = locate_header_row(tmp.name, expected_keywords=["non null"])
        assert result == 3

    os.unlink(tmp.name)
