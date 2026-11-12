-- Independent, non-destructive staging schema. Never alters legacy public tables.
-- Execute through a transaction with an authorized migration role.
CREATE SCHEMA IF NOT EXISTS portfolio_v2;
CREATE TABLE IF NOT EXISTS portfolio_v2.venues (
  venue_id TEXT PRIMARY KEY CHECK(length(venue_id) BETWEEN 1 AND 128),
  display_name TEXT NOT NULL CHECK(length(display_name) > 0)
);
CREATE TABLE IF NOT EXISTS portfolio_v2.wines (
  venue_id TEXT NOT NULL REFERENCES portfolio_v2.venues(venue_id),
  wine_id TEXT NOT NULL CHECK(length(wine_id)>0),
  wine_name TEXT NOT NULL CHECK(length(wine_name)>0),
  pronunciation TEXT NOT NULL CHECK(length(pronunciation)>0),
  origin TEXT NOT NULL CHECK(length(origin)>0),
  category TEXT NOT NULL CHECK(category IN ('red','white','sparkling','rose')),
  wine_type TEXT NOT NULL,
  sweetness TEXT NOT NULL CHECK(sweetness IN ('dry','semi-dry','semi-sweet','sweet')),
  characteristics JSONB NOT NULL DEFAULT '[]'::jsonb CHECK(jsonb_typeof(characteristics)='array'),
  pairings JSONB NOT NULL DEFAULT '[]'::jsonb CHECK(jsonb_typeof(pairings)='array'),
  available_by_glass BOOLEAN NOT NULL,
  available_by_bottle BOOLEAN NOT NULL,
  source_metadata TEXT,
  source_characteristics TEXT,
  source_body TEXT,
  source_pairings TEXT,
  CHECK (available_by_glass OR available_by_bottle),
  PRIMARY KEY (venue_id,wine_id)
);
CREATE INDEX IF NOT EXISTS wines_venue_category ON portfolio_v2.wines(venue_id,category,sweetness);
