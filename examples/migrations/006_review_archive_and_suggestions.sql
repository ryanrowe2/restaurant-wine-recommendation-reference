-- Additive v12 migration: fail closed on legacy noncanonical suggestion values.
-- Migration 005 replaced the validator function but did not inspect every
-- preexisting column bound to that function. Do not edit signed migration 005.
DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM portfolio_v2.wines
            WHERE NOT portfolio_v2.valid_tag_array(characteristics)
               OR NOT portfolio_v2.valid_tag_array(pairings)
               OR NOT portfolio_v2.valid_tag_array(suggested_pairings)) THEN
   RAISE EXCEPTION 'Noncanonical historical wine tags/suggestions require operator review before v12 migration';
 END IF;
END $$;

-- Reviewed enrichment must survive temporary empty inventory snapshots.
-- This table is independent of active recommendations; only active wines in
-- portfolio_v2.wines can be selected, never archived-only wine identities.
CREATE TABLE portfolio_v2.wine_review_archive (
 venue_id TEXT NOT NULL REFERENCES portfolio_v2.venues(venue_id),
 wine_id TEXT NOT NULL CHECK(length(btrim(wine_id)) BETWEEN 1 AND 1500),
 pronunciation TEXT NOT NULL CHECK(length(btrim(pronunciation)) BETWEEN 1 AND 1500),
 characteristics JSONB NOT NULL DEFAULT '[]'::jsonb,
 pairings JSONB NOT NULL DEFAULT '[]'::jsonb,
 suggested_pairings JSONB NOT NULL DEFAULT '[]'::jsonb,
 pairings_reviewed BOOLEAN NOT NULL DEFAULT FALSE,
 source_metadata TEXT,
 source_characteristics TEXT,
 source_body TEXT,
 source_pairings TEXT,
 CONSTRAINT wine_review_archive_tags_valid CHECK (
  portfolio_v2.valid_tag_array(characteristics)
  AND portfolio_v2.valid_tag_array(pairings)
  AND portfolio_v2.valid_tag_array(suggested_pairings)),
 PRIMARY KEY(venue_id,wine_id)
);
-- Backfill the active catalog atomically before any next import/replacement.
INSERT INTO portfolio_v2.wine_review_archive (
 venue_id,wine_id,pronunciation,characteristics,pairings,suggested_pairings,
 pairings_reviewed,source_metadata,source_characteristics,source_body,source_pairings)
SELECT venue_id,wine_id,pronunciation,characteristics,pairings,suggested_pairings,
 pairings_reviewed,source_metadata,source_characteristics,source_body,source_pairings
FROM portfolio_v2.wines;
