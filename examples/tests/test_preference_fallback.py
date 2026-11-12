"""Fallback, no-invention and repeatability tests for the example."""

import itertools
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import InsufficientCandidates, Preferences, load_catalog, select_candidates  # noqa: E402


class PreferenceFallbackTests(unittest.TestCase):
    def setUp(self):
        self.catalog = load_catalog()

    def test_relaxes_soft_preferences_without_losing_venue_or_format(self):
        prefs = Preferences(category="red", sweetness="dry", characteristics=("bold", "spice"), serving_format="glass")
        result = select_candidates(self.catalog, "venue_a", prefs)
        self.assertEqual(len(result.wines), 3)
        self.assertEqual(len({wine.wine_id for wine in result.wines}), 3)
        self.assertEqual({w.restaurant_id for w in result.wines}, {"venue_a"})
        self.assertTrue(all(w.available_by_glass for w in result.wines))
        self.assertGreater(len(result.stages_visited), 1)

    def test_deterministic_output(self):
        prefs = Preferences(category="red", sweetness="dry", serving_format="bottle")
        first = select_candidates(self.catalog, "venue_a", prefs)
        second = select_candidates(reversed(self.catalog), "venue_a", prefs)
        self.assertEqual(first, second)

    def test_sparse_inventory_cannot_fabricate_three(self):
        with self.assertRaises(InsufficientCandidates):
            select_candidates(self.catalog, "venue_sparse", Preferences(serving_format="either"))

    def test_unknown_venue_is_not_backfilled_from_others(self):
        with self.assertRaises(InsufficientCandidates):
            select_candidates(self.catalog, "unknown_venue", Preferences())

    def test_many_descriptors_do_not_generate_exponential_query_stages(self):
        prefs = Preferences(category="unavailable", sweetness="sweet", characteristics=tuple(f"tag_{i}" for i in range(128)))
        result = select_candidates(self.catalog, "venue_a", prefs)
        self.assertLessEqual(len(result.stages_visited), 18)
        self.assertEqual(len(result.wines), 3)

    def test_invalid_preference_types_are_rejected_clearly(self):
        for kwargs in ({"category":None}, {"characteristics":"bold"}, {"serving_format":None}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                Preferences(**kwargs)

    def test_exhaustive_small_preference_matrix_never_crosses_hard_constraints(self):
        tested = 0
        for venue, serving, category, sweetness, descriptors in itertools.product(
            ("venue_a", "venue_b", "venue_sparse", "unknown_venue"),
            ("glass", "bottle", "either"),
            ("red", "white", "other", ""),
            ("sweet", "dry", ""),
            ((), ("bold",), ("floral", "spice")),
        ):
            preferences = Preferences(
                category=category, sweetness=sweetness, characteristics=descriptors, serving_format=serving
            )
            try:
                result = select_candidates(self.catalog, venue, preferences)
            except InsufficientCandidates:
                continue
            tested += 1
            self.assertEqual(len(result.wines), 3)
            self.assertEqual(len({wine.wine_id for wine in result.wines}), 3)
            self.assertTrue(all(wine.restaurant_id == venue for wine in result.wines))
            self.assertTrue(all(
                wine.available_by_glass if serving == "glass" else
                wine.available_by_bottle if serving == "bottle" else
                wine.available_by_glass or wine.available_by_bottle
                for wine in result.wines
            ))
        self.assertGreater(tested, 100)


if __name__ == "__main__":
    unittest.main()
