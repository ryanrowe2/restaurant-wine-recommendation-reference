"""Scope and serving-format eligibility tests for the independent example."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import Preferences, format_eligible, load_catalog, select_candidates  # noqa: E402


class RestaurantIsolationTests(unittest.TestCase):
    def setUp(self):
        self.catalog = load_catalog()

    def test_no_cross_venue_wines_even_after_preference_relaxation(self):
        prefs = Preferences(category="nonexistent", sweetness="sweet", characteristics=("tobacco",), serving_format="either")
        result = select_candidates(self.catalog, "venue_a", prefs)
        self.assertEqual(len(result.wines), 3)
        self.assertTrue(all(w.restaurant_id == "venue_a" for w in result.wines))
        self.assertTrue(any("category=*" in stage for stage in result.stages_visited))

    def test_glass_only_excludes_bottle_only_even_when_other_flag_is_false(self):
        result = select_candidates(self.catalog, "venue_a", Preferences(serving_format="glass"))
        self.assertTrue(all(w.available_by_glass for w in result.wines))
        self.assertNotIn("a05", {w.wine_id for w in result.wines})
        bottle_only = next(w for w in self.catalog if w.wine_id == "a05")
        self.assertFalse(format_eligible(bottle_only, "glass"))

    def test_bottle_only_is_eligible_for_bottle_request(self):
        prefs = Preferences(category="red", sweetness="dry", characteristics=("spice",), serving_format="bottle")
        result = select_candidates(self.catalog, "venue_a", prefs)
        self.assertIn("a05", {w.wine_id for w in result.wines})
        self.assertTrue(all(w.available_by_bottle for w in result.wines))

    def test_invalid_serving_format_fails_closed(self):
        with self.assertRaises(ValueError):
            select_candidates(self.catalog, "venue_a", Preferences(serving_format="none"))


if __name__ == "__main__":
    unittest.main()
