-- Canonical tag contract used by SQLite storage and Python Wine tags.
-- A prior CHECK already calls valid_tag_array; replacing its immutable body
-- requires explicitly inspecting existing rows and rejecting noncanonical data.
CREATE OR REPLACE FUNCTION portfolio_v2.valid_tag_array(tags jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS $$
 SELECT CASE WHEN jsonb_typeof(tags) <> 'array' THEN false
             WHEN jsonb_array_length(tags) > 128 THEN false
             ELSE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(tags) AS t(x)
                 WHERE jsonb_typeof(t.x) <> 'string'
                 OR (t.x #>> '{}') !~ '^[a-z0-9_-]{1,48}$') END
$$;
DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM portfolio_v2.wines
            WHERE NOT portfolio_v2.valid_tag_array(characteristics)
               OR NOT portfolio_v2.valid_tag_array(pairings)) THEN
   RAISE EXCEPTION 'Noncanonical persisted wine tags require operator review';
 END IF;
END $$;
