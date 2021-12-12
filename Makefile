EXTENSION = datafly_anon
EXTVERSION   = 1.0
DISTVERSION  = 0.1.0

DATA = sql/datafly_anon--1.6.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

dist:
	git archive --format zip --prefix=$(EXTENSION)-$(DISTVERSION)/ -o $(EXTENSION)-$(DISTVERSION).zip
