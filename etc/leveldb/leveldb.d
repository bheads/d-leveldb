module etc.leveldb.leveldb;

public import
    etc.leveldb.db,
    etc.leveldb.exceptions,
    etc.leveldb.slice,
    etc.leveldb.options;

private import deimos.leveldb.leveldb;

public __gshared immutable int LEVELDB_MAJOR_VERSION, LEVELDB_MINOR_VERSION;

shared static this()
{
    LEVELDB_MAJOR_VERSION = leveldb_major_version();
    LEVELDB_MINOR_VERSION = leveldb_minor_version();
}

