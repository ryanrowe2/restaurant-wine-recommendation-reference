-- Strict enforcement of JSONB tags. Applies after 001, in a tracked migration.
CREATE OR REPLACE FUNCTION portfolio_v2.valid_tag_array(tags jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS $$
 SELECT CASE WHEN jsonb_typeof(tags) <> 'array' THEN false
             WHEN jsonb_array_length(tags) > 128 THEN false
             ELSE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(tags) AS t(x)
                 WHERE jsonb_typeof(t.x) <> 'string'
                 OR (t.x #>> '{}') !~ '^[a-z0-9_ -]{1,48}$') END
$$;
ALTER TABLE portfolio_v2.wines ADD CONSTRAINT wine_characteristics_valid
 CHECK(portfolio_v2.valid_tag_array(characteristics));
ALTER TABLE portfolio_v2.wines ADD CONSTRAINT wine_pairings_valid
 CHECK(portfolio_v2.valid_tag_array(pairings));
