module leveldb;

private:
    import deimos.leveldb.leveldb;

public:
    import leveldb.db,
        leveldb.exceptions,
        leveldb.slice,
        leveldb.options;


__gshared immutable int LEVELDB_MAJOR_VERSION, LEVELDB_MINOR_VERSION;

shared static this()
{
    // Load the version information
    LEVELDB_MAJOR_VERSION = leveldb_major_version();
    LEVELDB_MINOR_VERSION = leveldb_minor_version();
}

