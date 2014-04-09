module leveldb;

public import
    leveldb.db,
    leveldb.exceptions,
    leveldb.slice,
    leveldb.options;

private import deimos.leveldb.leveldb;

public __gshared immutable int LEVELDB_MAJOR_VERSION, LEVELDB_MINOR_VERSION;

shared static this()
{
    LEVELDB_MAJOR_VERSION = leveldb_major_version();
    LEVELDB_MINOR_VERSION = leveldb_minor_version();
}

