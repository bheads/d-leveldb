/**
 * D-LevelDB Options
 *
 * Database config, read and write options
 *
 * Copyright: Copyright © 2013 Byron Heads
 * License: <a href="http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors: Byron Heads
*/
/*          Copyright  © 2013 Byron Heads
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
module leveldb.options;

private:
    import std.algorithm : cmp;
    import std.string : toStringz;

    import deimos.leveldb.leveldb,
        leveldb.exceptions;

/// Create default init read and write options
public:
    __gshared const(ReadOptions) DefaultReadOptions;
    __gshared const(WriteOptions) DefaultWriteOptions;

shared static this()
{
    DefaultReadOptions = new ReadOptions;
    DefaultWriteOptions = new WriteOptions;
}

/**
 * Database creation and general usgage options
 */
class Options
{
private:
    leveldb_options_t _opt = null;

    /// Store a copy of these objects so they are not cleaned up by the GC
    Environment _env;
    Cache _cache;
    FilterPolicy _filter;
    Comparator _comparator;

package:
    @property 
    inout(leveldb_options_t) ptr() inout
    {
        return _opt;
    }

public:

    /// Create the internal option object
    this()
    {
        _opt = dbEnforce(leveldb_options_create(), "Failed to create an option");
    }

    /// Destroy any valid option pointer
    ~this()
    {
        if(valid)
        {
            leveldb_options_destroy(_opt);
            _opt = null;
        }
    }

    /** If true, the database will be created if it is missing.
      * Default: false
      */
    @property
    void create_if_missing(bool val)
    {
        leveldb_options_set_create_if_missing(_opt, val);
    }

    /** If true, an error is raised if the database already exists.
     *  Default: false
     */
    @property
    void error_if_missing(bool val)
    {
        leveldb_options_set_error_if_exists(_opt, val);
    }

    /** If true, the implementation will do aggressive checking of the
     *  data it is processing and will stop early if it detects any
     *  errors.  This may have unforeseen ramifications: for example, a
     *  corruption of one DB entry may cause a large number of entries to
     *  become unreadable or for the entire DB to become unopenable.
     *  Default: false
     */
    @property
    void paranoid_checks(bool val)
    {
        leveldb_options_set_paranoid_checks(_opt, val);
    }

    /** Compress blocks using the specified compression algorithm.  This
     *  parameter can be changed dynamically.
     *
     * leveldb_no_compression = 0,
     * leveldb_snappy_compression = 1
     *
     *  Default: kSnappyCompression, which gives lightweight but fast
     *  compression.
     *
     *  Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
     *      ~200-500MB/s compression
     *      ~400-800MB/s decompression
     *  Note that these speeds are significantly faster than most
     *  persistent storage speeds, and therefore it is typically never
     *  worth switching to kNoCompression.  Even if the input data is
     *  incompressible, the kSnappyCompression implementation will
     *  efficiently detect that and will switch to uncompressed mode.
     */
    @property
    void compression(int val)
    {
        leveldb_options_set_compression(_opt, val);
    }

    /** Parameters that affect performance
     *  Amount of data to build up in memory (backed by an unsorted log
     *  on disk) before converting to a sorted on-disk file.
     *
     *  Larger values increase performance, especially during bulk loads.
     *  Up to two write buffers may be held in memory at the same time,
     *  so you may wish to adjust this parameter to control memory usage.
     *  Also, a larger write buffer will result in a longer recovery time
     *  the next time the database is opened.
     *
     * Default: 4MB
     */
    @property
    void write_buffer_size(size_t size)
    {
        leveldb_options_set_write_buffer_size(_opt, size);
    }

    /** Number of open files that can be used by the DB.  You may need to
     *  increase this if your database has a large working set (budget
     *  one open file per 2MB of working set).
     *
     *  Default: 1000
     */
    @property
    void max_open_files(int val)
    {
        leveldb_options_set_max_open_files(_opt, val);
    }

    /** Approximate size of user data packed per block.  Note that the
     *  block size specified here corresponds to uncompressed data.  The
     *  actual size of the unit read from disk may be smaller if
     *  compression is enabled.  This parameter can be changed dynamically.
     * 
     *  Default: 4K
     */
    @property
    void block_size(size_t size)
    {
        leveldb_options_set_block_size(_opt, size);
    }

    /** Number of keys between restart points for delta encoding of keys.
     *  This parameter can be changed dynamically.  Most clients should
     *  leave this parameter alone.
     *
     * Default: 16
     */
    @property
    void block_restart_interval(int val)
    {
        leveldb_options_set_block_restart_interval(_opt, val);
    }

    /** Use the specified object to interact with the environment,
     *  e.g. to read/write files, schedule background work, etc.
     *  Default: Environment()
     */
    @property
    void env(Environment env)
    {
        _env = env; // save pointer so gc doesn't collect it
        if(env)
            leveldb_options_set_env(_opt, env._env);
        else
            leveldb_options_set_env(_opt, null);
    }

    /** Control over blocks (user data is stored in a set of blocks, and
     *  a block is the unit of reading from disk).
     *
     *  If non-NULL, use the specified cache for blocks.
     *  If null, leveldb will automatically create and use an 8MB internal cache.
     *  Default: null
     */
    @property
    void cache(Cache cache)
    {
        _cache = cache; // save pointer so gc doesn't collect it
        if(cache)
            leveldb_options_set_cache(_opt, cache.ptr);
        else
            leveldb_options_set_cache(_opt, null);
    }

    /** If non-NULL, use the specified filter policy to reduce disk reads.
     *  Many applications will benefit from passing the result of
     *  BloomFilterPolicy here.
     *   
     *  Default: null
     */
    @property
    void filter_policy(FilterPolicy filter)
    {
        _filter = filter; // save pointer so gc doesn't collect it
        if(filter)
            leveldb_options_set_filter_policy(_opt, filter.ptr);
        else
            leveldb_options_set_filter_policy(_opt, null);
    }

    /** Comparator used to define the order of keys in the table.
     *  Default: a comparator that uses lexicographic byte-wise ordering
     *   
     *  REQUIRES: The client must ensure that the comparator supplied
     *  here has the same name and orders keys *exactly* the same as the
     *  comparator provided to previous open calls on the same DB.
     */
    @property
    void comparator(Comparator comparator)
    {
        _comparator = comparator; // save pointer so gc doesn't collect it
        if(comparator)
            leveldb_options_set_comparator(_opt, comparator._comp);
        else
            leveldb_options_set_comparator(_opt, null);
    }

    /// indicates if the option has been created
    @property 
    bool valid() inout
    {
        return _opt !is null;
    }

    // Optional Sub objects

    /// Environment object, API only has deault environment
    static class Environment
    {
    private:
        leveldb_env_t _env;

    public:
        this()
        {
            _env = dbEnforce(leveldb_create_default_env(), "Failed to create leveldb environment");
        }

        ~this()
        {
            leveldb_env_destroy(_env);
        }
    }

    abstract static class Cache
    {
        @property 
        inout(leveldb_cache_t) ptr() inout;
    }

    /// Cache Object, can only set size
    static class LRUCache : Cache
    {
    private:
        leveldb_cache_t _cache;

    public:

        this()
        {
            /// Create a default cache 10MB
            this(10 * 1048576);
        }

        this(size_t capacity)
        {
            _cache = dbEnforce(leveldb_cache_create_lru(capacity), "Failed to create leveldb cache");
        }

        ~this()
        {
            leveldb_cache_destroy(_cache);
        }

        @property
        override inout(leveldb_cache_t) ptr() inout
        {
            return _cache;
        }
    }

    abstract static class FilterPolicy
    {
        @property
        inout(leveldb_filterpolicy_t) ptr() inout;
    }

    /// Bloom filter
    static class BloomFilterPolicy : FilterPolicy
    {
    private:
        leveldb_filterpolicy_t _filter;

    public:

        this()
        {
            /// Create a default bloom filter
            this(10);
        }

        this(int bits_per_key)
        {
            _filter = dbEnforce(leveldb_filterpolicy_create_bloom(bits_per_key), "Failed to create leveldb bloom filter");
        }

        ~this()
        {
            leveldb_filterpolicy_destroy(_filter);
        }

        @property
        override inout(leveldb_filterpolicy_t) ptr() inout
        {
            return _filter;
        }
    }

    /// User Filter Policy
    abstract static class AFilterPolicy : FilterPolicy
    {
    private:
        leveldb_filterpolicy_t _filter;

    public:

        this()
        {
            _filter = dbEnforce(leveldb_filterpolicy_create(cast(void*)this, &filterDestructor, &filterCreate, &filterKeyMayMatch, &filterName), "Failed to create leveldb filter");
        }

        ~this()
        {
            leveldb_filterpolicy_destroy(_filter);
        }

        void destructor();
        char* create(const const(char)* key_array, const size_t* key_length_array,
            int num_keys, size_t* filter_length);
        ubyte match(const char[]key, const char[] filter);
        string name();

        @property
        override inout(leveldb_filterpolicy_t) ptr() inout
        {
            return _filter;
        }
    }

    /// User Comparator, this is a default string comparator
    static class Comparator
    {
    private:
        leveldb_comparator_t _comp;

    public:

        this()
        {
            _comp = dbEnforce(leveldb_comparator_create(cast(void*)this, &compareDestructor, &compareCompare, &compareName), "Failed to create leveldb comparator");
        }

        ~this()
        {
            leveldb_comparator_destroy(_comp);
        }

        void destructor() inout
        {}

        int compare(const char[] a, const char[] b) inout
        {
            return cmp(a, b);
        }

        string name() inout
        {
            return "String Compare";
        }
    }
}

package abstract class ASnapshot
{
public:
    @property
    inout(leveldb_snapshot_t) ptr() inout;
}

/// Controls database reading options
class ReadOptions
{
private:
    leveldb_readoptions_t _opt;

package:
    @property 
    inout(leveldb_readoptions_t) ptr() inout
    {
        return _opt;
    }

public:
    /// Create the internal option object
    this()
    {
        _opt = dbEnforce(leveldb_readoptions_create(),"Failed to create an read options");
    }

    /// Destroy any valid option pointer
    ~this()
    {
        if(valid)
        {
            leveldb_readoptions_destroy(_opt);
            _opt = null;
        }
    }

    /** If true, all data read from underlying storage will be
     *  verified against corresponding checksums.
     *  Default: false
     */
    @property
    void verify_checksums(bool val)
    {
        leveldb_readoptions_set_verify_checksums(_opt, val);
    }

    /** Should the data read for this iteration be cached in memory?
     *  Callers may wish to set this field to false for bulk scans.
     *  Default: true
     */
    @property
    void fill_cache(bool val)
    {
        leveldb_readoptions_set_fill_cache(_opt, val);
    }

    /** If "snapshot" is non-NULL, read as of the supplied snapshot
     *  (which must belong to the DB that is being read and which must
     *  not have been released).  If "snapshot" is null, use an impliicit
     *  snapshot of the state at the beginning of this read operation.
     *  Default: null
     */
    @property
    void snapshot(const(ASnapshot) snapshot)
    {
        leveldb_readoptions_set_snapshot(_opt, snapshot.ptr);
    }

    /// indicates if the option has been created
    @property 
    bool valid() inout
    {
        return _opt !is null;
    }
}

/// Controls db writting
class WriteOptions
{
private:
    leveldb_writeoptions_t _opt;

package:
    @property
    inout(leveldb_writeoptions_t) ptr() inout
    {
        return _opt;
    }

public:
    /// Create the internal option object
    this()
    {
        _opt = dbEnforce(leveldb_writeoptions_create(), "Failed to create an read options");
    }

    /// Destroy any valid option pointer
    ~this()
    {
        if(valid)
        {
            leveldb_writeoptions_destroy(_opt);
            _opt = null;
        }
    }

    /** If true, the write will be flushed from the operating system
     *  buffer cache (by calling WritableFile::Sync()) before the write
     *  is considered complete.  If this flag is true, writes will be
     *  slower.
     *   
     *  If this flag is false, and the machine crashes, some recent
     *  writes may be lost.  Note that if it is just the process that
     *  crashes (i.e., the machine does not reboot), no writes will be
     *  lost even if sync==false.
     *   
     *  In other words, a DB write with sync==false has similar
     *  crash semantics as the "write()" system call.  A DB write
     *  with sync==true has similar crash semantics to a "write()"
     *  system call followed by "fsync()".
     *   
     *  Default: false
     */
    @property
    void sync(bool val)
    {
        leveldb_writeoptions_set_sync(_opt, val);
    }

    /// indicates if the option has been created
    @property
    bool valid() inout
    {
        return _opt !is null;
    }
}

//* Leveldb C API Callback handlers */
private:
extern(C):
    void compareDestructor(void* state)
    {
        auto c = cast(Options.Comparator*)state;
        c.destructor();
    }

    int compareCompare(void* state, const char* a, size_t alen, const char* b, size_t blen)
    {
        auto c = cast(Options.Comparator*)state;
        return c.compare(a[0..alen], b[0..blen]);
    }

    const(char*) compareName(void* state)
    {
        auto c = cast(Options.Comparator*)state;
        return toStringz(c.name());
    }

    void filterDestructor(void* state)
    {
        auto f = cast(Options.AFilterPolicy*)state;
        f.destructor();
    }

    char* filterCreate(void* state, const const(char)* key_array, 
        const size_t* key_length_array, int num_keys, size_t* filter_length)
    {
        auto f = cast(Options.AFilterPolicy*)state;
        return f.create(key_array, key_length_array, num_keys, filter_length);
    }

    ubyte filterKeyMayMatch(void* state, const char* key, size_t length, const char* filter,
        size_t filter_length)
    {
        auto f = cast(Options.AFilterPolicy*)state;
        return f.match(key[0..length], filter[0..filter_length]);
    }

    const(char*) filterName(void* state)
    {
        auto f = cast(Options.AFilterPolicy*)state;
        return toStringz(f.name());
    }
