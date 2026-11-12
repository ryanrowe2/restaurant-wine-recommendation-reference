-- v15 additive required-text validation: never rewrite the signed 004 migration.
-- PostgreSQL TEXT disallows U+0000; all other Unicode Cc and non-ASCII-space
-- whitespace code points are explicitly rejected, exactly as the Python domain.
-- This SQL function is immutable and STRICT so CHECK returns false on bad TEXT.
CREATE FUNCTION portfolio_v2.valid_required_wine_text(value TEXT)
RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE STRICT AS $palate$
  SELECT length(value) BETWEEN 1 AND 1500
     AND length(btrim(value)) >= 1
     AND translate(value,
         chr(1) ||
         chr(2) ||
         chr(3) ||
         chr(4) ||
         chr(5) ||
         chr(6) ||
         chr(7) ||
         chr(8) ||
         chr(9) ||
         chr(10) ||
         chr(11) ||
         chr(12) ||
         chr(13) ||
         chr(14) ||
         chr(15) ||
         chr(16) ||
         chr(17) ||
         chr(18) ||
         chr(19) ||
         chr(20) ||
         chr(21) ||
         chr(22) ||
         chr(23) ||
         chr(24) ||
         chr(25) ||
         chr(26) ||
         chr(27) ||
         chr(28) ||
         chr(29) ||
         chr(30) ||
         chr(31) ||
         chr(127) ||
         chr(128) ||
         chr(129) ||
         chr(130) ||
         chr(131) ||
         chr(132) ||
         chr(133) ||
         chr(134) ||
         chr(135) ||
         chr(136) ||
         chr(137) ||
         chr(138) ||
         chr(139) ||
         chr(140) ||
         chr(141) ||
         chr(142) ||
         chr(143) ||
         chr(144) ||
         chr(145) ||
         chr(146) ||
         chr(147) ||
         chr(148) ||
         chr(149) ||
         chr(150) ||
         chr(151) ||
         chr(152) ||
         chr(153) ||
         chr(154) ||
         chr(155) ||
         chr(156) ||
         chr(157) ||
         chr(158) ||
         chr(159) ||
         chr(160) ||
         chr(5760) ||
         chr(8192) ||
         chr(8193) ||
         chr(8194) ||
         chr(8195) ||
         chr(8196) ||
         chr(8197) ||
         chr(8198) ||
         chr(8199) ||
         chr(8200) ||
         chr(8201) ||
         chr(8202) ||
         chr(8232) ||
         chr(8233) ||
         chr(8239) ||
         chr(8287) ||
         chr(12288), '') = value
$palate$;

ALTER TABLE portfolio_v2.wines
 ADD CONSTRAINT wine_required_text_contract_v15 CHECK (
   portfolio_v2.valid_required_wine_text(wine_id) AND
   portfolio_v2.valid_required_wine_text(wine_name) AND
   portfolio_v2.valid_required_wine_text(pronunciation) AND
   portfolio_v2.valid_required_wine_text(origin) AND
   portfolio_v2.valid_required_wine_text(wine_type)
 );
