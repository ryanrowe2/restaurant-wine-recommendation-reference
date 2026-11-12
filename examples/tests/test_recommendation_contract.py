"""Tests ensure generated presentation fields cannot bypass the catalog."""

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import (  # noqa: E402
    ContractError,
    Preferences,
    create_deterministic_recommendations,
    load_catalog,
    select_candidates,
    validate_recommendation_contract,
)


class RecommendationContractTests(unittest.TestCase):
    def setUp(self):
        self.prefs = Preferences(category="red", sweetness="dry", serving_format="glass", pairing="red_meat")
        self.selection = select_candidates(load_catalog(), "venue_a", self.prefs)
        self.recs = create_deterministic_recommendations(self.selection, self.prefs)

    def test_three_eligible_distinct_records(self):
        validate_recommendation_contract(self.recs, self.selection.wines)
        self.assertEqual(len({r["wine_id"] for r in self.recs}), 3)

    def test_unknown_id_is_rejected(self):
        bad = copy.deepcopy(self.recs)
        bad[0]["wine_id"] = "outside_catalog"
        with self.assertRaises(ContractError):
            validate_recommendation_contract(bad, self.selection.wines)

    def test_duplicate_id_is_rejected(self):
        bad = copy.deepcopy(self.recs)
        bad[1] = copy.deepcopy(bad[0])
        with self.assertRaises(ContractError):
            validate_recommendation_contract(bad, self.selection.wines)

    def test_missing_field_and_count_rejected(self):
        bad = copy.deepcopy(self.recs)
        bad[0].pop("description")
        with self.assertRaises(ContractError):
            validate_recommendation_contract(bad, self.selection.wines)
        with self.assertRaises(ContractError):
            validate_recommendation_contract(self.recs[:2], self.selection.wines)

    def test_misleading_catalog_attribute_rejected(self):
        bad = copy.deepcopy(self.recs)
        bad[0]["wine_name"] = "Invented swap"
        with self.assertRaises(ContractError):
            validate_recommendation_contract(bad, self.selection.wines)

    def test_invented_pronunciation_is_rejected(self):
        bad = copy.deepcopy(self.recs)
        bad[0]["pronunciation"] = "Misleading pronunciation"
        with self.assertRaises(ContractError):
            validate_recommendation_contract(bad, self.selection.wines)

    def test_oversized_explanation_is_rejected(self):
        bad = copy.deepcopy(self.recs)
        bad[0]["description"] = "x" * 1501
        with self.assertRaises(ContractError):
            validate_recommendation_contract(bad, self.selection.wines)

    def test_additional_unexpected_field_rejected(self):
        bad = copy.deepcopy(self.recs)
        bad[0]["unverified_price"] = "$0"
        with self.assertRaises(ContractError):
            validate_recommendation_contract(bad, self.selection.wines)


if __name__ == "__main__":
    unittest.main()
