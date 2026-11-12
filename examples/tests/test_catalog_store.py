"""Atomic replace tests, including a forced failure after the delete step."""

import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from catalog_store import CatalogStore  # noqa: E402
from pipeline import load_catalog  # noqa: E402


class CatalogStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = CatalogStore(Path(self.temp.name) / "catalog.sqlite3")
        self.catalog = load_catalog()
        self.a = tuple(w for w in self.catalog if w.restaurant_id == "venue_a")
        self.b = tuple(w for w in self.catalog if w.restaurant_id == "venue_b")

    def test_replacement_is_scoped_and_complete(self):
        self.store.replace_venue("venue_a", self.a)
        self.store.replace_venue("venue_b", self.b)
        updated = (replace(self.a[0], wine_name="Revised Fictional Wine"),)
        self.store.replace_venue("venue_a", updated)
        self.assertEqual(self.store.names_for_venue("venue_a"), ("Revised Fictional Wine",))
        self.assertEqual(len(self.store.names_for_venue("venue_b")), len(self.b))

    def test_empty_or_cross_venue_import_does_not_delete_existing_rows(self):
        self.store.replace_venue("venue_a", self.a)
        original = self.store.names_for_venue("venue_a")
        for bad_rows in (
            (), self.b[:1], (self.a[0], self.a[0]),
        ):
            with self.assertRaises(ValueError):
                self.store.replace_venue("venue_a", bad_rows)
            self.assertEqual(self.store.names_for_venue("venue_a"), original)
        # Invalid objects fail immediately at construction, before they can reach storage.
        for kwargs in ({"available_by_glass":False,"available_by_bottle":False},{"available_by_glass":1}):
            with self.assertRaises(ValueError):
                replace(self.a[0],**kwargs)
        self.assertEqual(self.store.names_for_venue("venue_a"),original)

    def test_sql_failure_after_delete_rolls_back_entire_replacement(self):
        self.store.replace_venue("venue_a", self.a)
        original = self.store.names_for_venue("venue_a")
        with self.store._connect() as conn:
            conn.execute("""CREATE TRIGGER abort_catalog_insert BEFORE INSERT ON catalog
                WHEN NEW.restaurant_id = 'venue_a'
                BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END;""")
        with self.assertRaises(Exception):
            self.store.replace_venue("venue_a", (replace(self.a[0], wine_name="New"),))
        self.assertEqual(self.store.names_for_venue("venue_a"), original)


if __name__ == "__main__":
    unittest.main()
