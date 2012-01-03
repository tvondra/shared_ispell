MODULE_big = shared_ispell
OBJS = src/shared_ispell.o src/spell.o

EXTENSION = shared_ispell
DATA = sql/shared_ispell--1.0.0.sql
MODULES = shared_ispell

CFLAGS=`pg_config --includedir-server`

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: shared_ispell.so

shared_ispell.so: $(OBJS)

%.o : src/%.c
