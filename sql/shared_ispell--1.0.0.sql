CREATE OR REPLACE FUNCTION shared_dispell_init(internal)
	RETURNS internal
	AS 'MODULE_PATHNAME', 'dispell_init'
	LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION shared_dispell_lexize(internal,internal,internal,internal)
	RETURNS internal
	AS 'MODULE_PATHNAME', 'dispell_lexize'
	LANGUAGE C IMMUTABLE;

CREATE TEXT SEARCH TEMPLATE shared_ispell (
    INIT = shared_dispell_init,
    LEXIZE = shared_dispell_lexize
);

CREATE TEXT SEARCH DICTIONARY czech_shared (
	TEMPLATE = shared_ispell,
	DictFile = czech,
	AffFile = czech,
	StopWords = czech
);

CREATE TEXT SEARCH CONFIGURATION public.czech_shared ( COPY = pg_catalog.simple );

ALTER TEXT SEARCH CONFIGURATION czech_shared
    ALTER MAPPING FOR asciiword, asciihword, hword_asciipart,
                      word, hword, hword_part
    WITH czech_shared;
