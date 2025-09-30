# tests/mock_gspread.py
# Minimal gspread-like mocks for offline testing of bot_logic.update_tenant_month_row


import re
import numpy as np

def _a1_to_rowcol(a1: str):
    m = re.match(r"^\$?([A-Za-z]+)\$?(\d+)$", a1)
    if not m:
        raise ValueError(f"Bad A1: {a1}")
    col_letters, row = m.group(1).upper(), int(m.group(2))
    col = 0
    for ch in col_letters:
        col = col * 26 + (ord(ch) - 64)
    return row, col

def _a1_guess(a1: str):
    return (int(a1), 1) if re.match(r"^\d+$", a1) else _a1_to_rowcol(a1)

def _ensure_size(grid, rows, cols):
    while len(grid) < rows:
        grid.append([""] * (len(grid[0]) if grid else cols))
    for r in range(len(grid)):
        if len(grid[r]) < cols:
            grid[r] += [""] * (cols - len(grid[r]))

class MockSpreadsheet:
    def batch_update(self, _req):  # conditional formatting no-op
        return {"ok": True}

class MockWorksheet:
    def __init__(self, title: str, values_2d):
        self.title = title
        self._vals = [
            [("" if (isinstance(x, float) and np.isnan(x)) else ("" if x is None else str(x))) for x in row]
            for row in values_2d
        ]
        self.id = abs(hash(title)) & 0x7FFFFFFF
        self.spreadsheet = MockSpreadsheet()
        self.row_count = max(100, len(self._vals))
        self.col_count = max(26, max((len(r) for r in self._vals), default=0))

    def get_all_values(self):
        return [row[:] for row in self._vals]

    def update(self, range_a1, values, value_input_option=None):
        if ":" in range_a1:
            start, end = range_a1.split(":")
            sr, sc = _a1_guess(start); er, ec = _a1_guess(end)
        else:
            sr, sc = _a1_guess(range_a1)
            er, ec = sr, sc + len(values[0]) - 1
        rows, cols = er - sr + 1, ec - sc + 1
        _ensure_size(self._vals, er, ec)
        for i in range(rows):
            row_vals = values[i] if i < len(values) else [""] * cols
            for j in range(cols):
                v = row_vals[j] if j < len(row_vals) else ""
                self._vals[sr - 1 + i][sc - 1 + j] = v
        self.row_count = len(self._vals)
        self.col_count = max(self.col_count, len(self._vals[0]))
        return True

    def batch_update(self, updates, value_input_option=None):
        if isinstance(updates, dict):  # formatting requests
            return True
        for item in updates:
            self.update(item["range"], item["values"], value_input_option=value_input_option)
        return True

    def append_rows(self, rows, value_input_option=None):
        for r in rows:
            self._vals.append([str(x) for x in r])
        self.row_count = len(self._vals)
        self.col_count = max(self.col_count, len(self._vals[0]))

    def add_rows(self, n):
        _ensure_size(self._vals, len(self._vals) + n, len(self._vals[0]))
        self.row_count = len(self._vals)

    def add_cols(self, n):
        for r in range(len(self._vals)):
            self._vals[r] += [""] * n
        self.col_count = len(self._vals[0])

    def sort(self, spec):  # no-op for tests
        pass

    def freeze(self, rows=1):  # no-op for tests
        pass