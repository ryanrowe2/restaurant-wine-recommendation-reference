-- Reject blank/oversized required identity and display attributes at SQL level.
-- New additive migration; earlier signed migration hashes remain unchanged.
-- Existing violating records must be reviewed before deployment: migration
-- intentionally refuses to certify an invalid catalog.
ALTER TABLE portfolio_v2.wines ADD CONSTRAINT wine_required_text_valid CHECK (
  length(btrim(wine_id)) BETWEEN 1 AND 1500 AND length(wine_id) <= 1500 AND
  length(btrim(wine_name)) BETWEEN 1 AND 1500 AND length(wine_name) <= 1500 AND
  length(btrim(pronunciation)) BETWEEN 1 AND 1500 AND length(pronunciation) <= 1500 AND
  length(btrim(origin)) BETWEEN 1 AND 1500 AND length(origin) <= 1500 AND
  length(btrim(wine_type)) BETWEEN 1 AND 1500 AND length(wine_type) <= 1500
);
