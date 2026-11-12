"""Reject malformed catalog fixtures before a recommendation can be built."""

import csv
import io
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import CATALOG_PATH, load_catalog  # noqa: E402


class CatalogInputTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "fixture.csv"
        with CATALOG_PATH.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            self.columns = reader.fieldnames
            self.rows = list(reader)

    def save(self, rows=None, columns=None):
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=columns or self.columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(self.rows[:1] if rows is None else rows)
        self.path.write_text(buffer.getvalue(), encoding="utf-8")

    def test_missing_column_is_rejected(self):
        self.save(columns=self.columns[:-1])
        with self.assertRaisesRegex(ValueError, "header"):
            load_catalog(self.path)

    def test_empty_catalog_is_rejected(self):
        self.save(rows=[])
        with self.assertRaisesRegex(ValueError, "empty"):
            load_catalog(self.path)

    def test_unknown_availability_is_rejected(self):
        self.rows[0]["available_by_glass"] = "sometimes"
        self.save()
        with self.assertRaisesRegex(ValueError, "Unrecognized"):
            load_catalog(self.path)

    def test_both_unavailable_is_rejected(self):
        self.rows[0]["available_by_glass"] = "no"
        self.rows[0]["available_by_bottle"] = "no"
        self.save()
        with self.assertRaisesRegex(ValueError, "at least one"):
            load_catalog(self.path)

    def test_missing_required_cell_is_rejected(self):
        self.rows[0]["wine_name"] = " "
        self.save()
        with self.assertRaisesRegex(ValueError, "required"):
            load_catalog(self.path)

    def test_duplicate_scoped_identity_is_rejected(self):
        self.save(rows=[self.rows[0], self.rows[0]])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            load_catalog(self.path)

    def test_excess_field_is_rejected(self):
        self.save()
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write("one,two,three,extra,too,many,fields\n")
        with self.assertRaisesRegex(ValueError, "Malformed"):
            load_catalog(self.path)


if __name__ == "__main__":
    unittest.main()
