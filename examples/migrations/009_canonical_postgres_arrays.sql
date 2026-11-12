-- Additive v17: canonical unordered/duplicate PostgreSQL tags are invalid.
-- Operators must remediate historical noncanonical rows before this migration.
CREATE OR REPLACE FUNCTION portfolio_v2.valid_tag_array(tags jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS $palate$
 SELECT CASE WHEN jsonb_typeof(tags) <> 'array' THEN false
             WHEN jsonb_array_length(tags) > 128 THEN false
             ELSE NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(tags) WITH ORDINALITY AS t(x,ordinal)
                WHERE jsonb_typeof(x) <> 'string'
                   OR (x #>> '{}') !~ '^[a-z0-9_-]{1,48}$'
                   OR (ordinal > 1 AND ((x #>> '{}') COLLATE "C") <=
                       (SELECT (prior.x #>> '{}') COLLATE "C" FROM jsonb_array_elements(tags)
                        WITH ORDINALITY AS prior(x,ordinal)
                        WHERE prior.ordinal=t.ordinal-1))
              ) END
$palate$;
DO $palate$
BEGIN
 IF EXISTS (SELECT 1 FROM portfolio_v2.wines
     WHERE NOT portfolio_v2.valid_tag_array(characteristics)
        OR NOT portfolio_v2.valid_tag_array(pairings)
        OR NOT portfolio_v2.valid_tag_array(suggested_pairings)) OR
    EXISTS (SELECT 1 FROM portfolio_v2.wine_review_archive
     WHERE NOT portfolio_v2.valid_tag_array(characteristics)
        OR NOT portfolio_v2.valid_tag_array(pairings)
        OR NOT portfolio_v2.valid_tag_array(suggested_pairings)) THEN
   RAISE EXCEPTION 'Historical noncanonical tag arrays require explicit review';
 END IF;
END $palate$;
