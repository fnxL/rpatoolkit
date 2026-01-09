import logging
from python_calamine import CalamineWorkbook

from typing import Any

log = logging.getLogger(__name__)


def find_header_row(
    source: Any,
    sheet_id: int | None = None,
    sheet_name: str | None = None,
    max_rows: int = 200,
    expected_keywords: list[str] | None = None,
):
    """
    Finds the header row in an excel file by identifying the first row with maximum consecutive non-null values.

    Parameters
    ----------
    source : Any
        Path or file-like object to read
    sheet_id : int | None, optional
        0-based index of the sheet to read, by default None (Cannot be used with sheet_name)
    sheet_name : str | None, optional
        Name of the worksheet to read, by default None (Cannot be used with sheet_id)
    max_rows : int, optional
        Maximum number of rows to scan for header identification, by default 200
    expected_keywords : list[str] | None, optional
        List of keywords to look for in the header row. If a row contains all of these keywords, it is considered a header row, by default None

    Returns
    -------
    int
        Zero-based index of the first row with maximum consecutive non-null values.

        If expected_keywords is provided, this is the first row with all expected keywords and maximum consecutive non-null values.

    Raises
    ------
    ValueError
        If both sheet_id and sheet_name are provided.


    Notes
    -----
    - If first few rows are empty and the first non-empty row is the header, then simply use read_excel_sheet() or pl.read_excel() directly instead of finding the header row.

    - This function would be best suited when you want to read a filtered excel sheet using read_excel_sheet(visible_rows_only=True, header_row=header_row). OR If the header row is not the first non-empty row.
    """
    if sheet_id and sheet_name:
        raise ValueError("sheet_id and sheet_name cannot be both specified.")

    wb = CalamineWorkbook.from_object(source)
    if sheet_id:
        ws = wb.get_sheet_by_index(0)
    elif sheet_name:
        ws = wb.get_sheet_by_name(sheet_name)
    else:
        ws = wb.get_sheet_by_index(0)

    max_consecutive = 0
    header_row = 0
    for i, row in enumerate(ws.iter_rows()):
        if i > max_rows:
            log.debug("Reached max_rows limit")
            break

        consecutive_count = 0
        is_all_keywords_present = False

        if expected_keywords:
            # If expected keywords are provided, check if all of them are present in the row
            row_values = [str(value).strip().lower() for value in row if value]

            is_all_keywords_present = all(
                keyword.lower() in row_values for keyword in expected_keywords
            )

        # Check for non-null consecutive values
        for value in row:
            if value:
                consecutive_count += 1
            else:
                break

        if consecutive_count > max_consecutive:
            max_consecutive = consecutive_count
            header_row = i
            if expected_keywords and is_all_keywords_present:
                # This is the first row with all expected keywords, and highest consecutive non-null count, so its most likely the header row
                log.info(
                    "Found first header row with all expected keywords and maximum consecutive non-null values"
                )
                break

    log.info(
        f"Identified header row at index: {header_row} with {max_consecutive} consecutive non-null values"
    )
    return header_row
