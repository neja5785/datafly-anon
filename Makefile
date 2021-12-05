EXTENSION = datafly_anon
EXTVERSION   = 3.0
DISTVERSION  = 0.3.0

DATA = sql/datafly_anon--1.2.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

dist:
	git archive --format zip --prefix=$(EXTENSION)-$(DISTVERSION)/ -o $(EXTENSION)-$(DISTVERSION).zip
