-- Add explicit review gate and immutable identity alias mapping to staged legacy data.
ALTER TABLE portfolio_v2.wines ADD COLUMN suggested_pairings JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE portfolio_v2.wines ADD COLUMN pairings_reviewed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE portfolio_v2.wines ADD CONSTRAINT wine_suggested_pairings_valid
  CHECK(portfolio_v2.valid_tag_array(suggested_pairings));
CREATE TABLE portfolio_v2.legacy_identity_map (
  venue_id TEXT NOT NULL REFERENCES portfolio_v2.venues(venue_id),
  legacy_name TEXT NOT NULL CHECK(length(btrim(legacy_name))>0),
  wine_id TEXT NOT NULL CHECK(length(btrim(wine_id))>0),
  reviewed BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (venue_id,legacy_name)
);
-- Preserve all existing names as stable aliases, including v2 imports.
INSERT INTO portfolio_v2.legacy_identity_map(venue_id,legacy_name,wine_id,reviewed)
SELECT venue_id,regexp_replace(lower(btrim(wine_name)),'[[:space:]]+',' ','g'),wine_id,FALSE
  FROM portfolio_v2.wines WHERE source_pairings IS NOT NULL;
-- Treat all previously inferred pairings as unapproved until a human verifies them.
UPDATE portfolio_v2.wines
  SET suggested_pairings=pairings,pairings='[]'::jsonb,pairings_reviewed=FALSE
  WHERE source_pairings IS NOT NULL;
