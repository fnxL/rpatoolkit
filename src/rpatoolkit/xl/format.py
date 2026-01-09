from openpyxl.styles import Alignment, Border, Side
from openpyxl.worksheet.worksheet import Worksheet


def apply_borders(
    ws: Worksheet,
    style: str = "thin",
    color: str = "000000",
    text_center=True,
):
    border = Border(
        left=Side(style=style, color=color),
        right=Side(style=style, color=color),
        top=Side(style=style, color=color),
        bottom=Side(style=style, color=color),
    )
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row):
        for cell in row:
            cell.border = border
            if text_center:
                cell.alignment = Alignment(horizontal="center", vertical="center")
