module etc.leveldb.options;

private import std.algorithm : cmp;
private import std.string : toStringz;

private import deimos.leveldb.leveldb;

public __gshared const ReadOptions DefaultReadOptions;
public __gshared const WriteOptions DefaultWriteOptions;

static this()
{
    DefaultReadOptions = new ReadOptions;
    DefaultWriteOptions = new WriteOptions;
}

class Options
{
private:
    leveldb_options_t _opt;

    Environment _env;
    Cache _cache;
    FilterPolicy _filter;
    Comparator _comparator;

package:
    @property const const(leveldb_options_t) ptr()
    {
        return _opt;
    }

public:

    /// Create the internal option object
    this()
    {
        if((_opt = leveldb_options_create()) is null)
        {
            throw new Exception("Failed to create an option");
        }
    }

    /// Destroy any valid option pointer
    ~this()
    {
        if(valid)
        {
            auto tmp = _opt;
            _opt = null;
            leveldb_options_destroy(tmp);
        }
    }

    @property void create_if_missing(bool val)
    {
        if(valid)
            leveldb_options_set_create_if_missing(_opt, val);
    }

    @property void error_if_missing(bool val)
    {
        if(valid)
            leveldb_options_set_error_if_exists(_opt, val);
    }

    @property void paranoid_checks(bool val)
    {
        if(valid)
            leveldb_options_set_paranoid_checks(_opt, val);
    }

    /// leveldb_no_compression or leveldb_snappy_compression
    @property void compression(int val)
    {
        if(valid)
            leveldb_options_set_compression(_opt, val);
    }

    @property void write_buffer_size(size_t size)
    {
        if(valid)
            leveldb_options_set_write_buffer_size(_opt, size);
    }

    @property void max_open_files(int val)
    {
        if(valid)
            leveldb_options_set_max_open_files(_opt, val);
    }

    @property void block_size(size_t size)
    {
        if(valid)
            leveldb_options_set_block_size(_opt, size);
    }

    @property void block_restart_interval(int val)
    {
        if(valid)
            leveldb_options_set_block_restart_interval(_opt, val);
    }

    @property void env(Environment env)
    {
        if(valid)
        {
            _env = env; // save pointer so gc doesn't collect it
            if(env)
                leveldb_options_set_env(_opt, env._env);
            else
                leveldb_options_set_env(_opt, null);
        }
    }

    @property void cache(Cache cache)
    {
        if(valid)
        {
            _cache = cache; // save pointer so gc doesn't collect it
            if(cache)
                leveldb_options_set_cache(_opt, cache.ptr);
            else
                leveldb_options_set_cache(_opt, null);
        }
    }

    @property void filter_policy(FilterPolicy filter)
    {
        if(valid)
        {
            _filter = filter; // save pointer so gc doesn't collect it
            if(filter)
                leveldb_options_set_filter_policy(_opt, filter.ptr);
            else
                leveldb_options_set_filter_policy(_opt, null);
        }
    }

    @property void comparator(Comparator comparator)
    {
        if(valid)
        {
            _comparator = comparator; // save pointer so gc doesn't collect it
            if(comparator)
                leveldb_options_set_comparator(_opt, comparator._comp);
            else
                leveldb_options_set_comparator(_opt, null);
        }
    }

    /// indicates if the option has been created
    @property bool valid()
    {
        return _opt !is null;
    }



    /// Optional Sub objects

    /// Environment object, API only has deault environment
    static class Environment
    {
    private:
        leveldb_env_t _env;

    public:
        this()
        {
            if((_env = leveldb_create_default_env()) is null)
                throw new Exception("Failed to create leveldb environment");
        }

        ~this()
        {
            leveldb_env_destroy(_env);
        }
    }

    abstract static class Cache
    {
        @property leveldb_cache_t ptr();
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
            if((_cache = leveldb_cache_create_lru(capacity)) is null)
                throw new Exception("Failed to create leveldb cache");
        }

        ~this()
        {
            leveldb_cache_destroy(_cache);
        }

        @property override leveldb_cache_t ptr()
        {
            return _cache;
        }
    }

    abstract static class FilterPolicy
    {
        @property leveldb_filterpolicy_t ptr();
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
            if((_filter = leveldb_filterpolicy_create_bloom(bits_per_key)) is null)
                throw new Exception("Failed to create leveldb bloom filter");
        }

        ~this()
        {
            leveldb_filterpolicy_destroy(_filter);
        }

        @property override leveldb_filterpolicy_t ptr()
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
            if((_filter = leveldb_filterpolicy_create(cast(void*)this,
                &filterDestructor, &filterCreate, &filterKeyMayMatch, &filterName)) is null)
                throw new Exception("Failed to create leveldb filter");
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

        @property override leveldb_filterpolicy_t ptr()
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
            if((_comp = leveldb_comparator_create(cast(void*)this, 
                &compareDestructor, &compareCompare, &compareName)) is null)
                throw new Exception("Failed to create leveldb comparator");
        }

        ~this()
        {
            leveldb_comparator_destroy(_comp);
        }

        void destructor()
        {}

        int compare(const char[] a, const char[] b)
        {
            return cmp(a, b);
        }

        string name()
        {
            return "String Compare";
        }
    }
}

package abstract class ASnapshot
{
public:
    @property const const(leveldb_snapshot_t) ptr();
}

class ReadOptions
{
private:
    leveldb_readoptions_t _opt;

package:
    @property const const(leveldb_readoptions_t) ptr()
    {
        return _opt;
    }

public:
    /// Create the internal option object
    this()
    {
        if((_opt = leveldb_readoptions_create()) is null)
        {
            throw new Exception("Failed to create an read options");
        }
    }

    /// Destroy any valid option pointer
    ~this()
    {
        if(valid)
        {
            auto tmp = _opt;
            _opt = null;
            leveldb_readoptions_destroy(tmp);
        }
    }

    @property void verify_checksums(bool val)
    {
        if(valid)
            leveldb_readoptions_set_verify_checksums(_opt, val);
    }

    @property void fill_cache(bool val)
    {
        if(valid)
            leveldb_readoptions_set_fill_cache(_opt, val);
    }

    @property void snapshot(const ASnapshot snapshot)
    {
        if(valid)
            leveldb_readoptions_set_snapshot(_opt, snapshot.ptr);
    }

    /// indicates if the option has been created
    @property bool valid()
    {
        return _opt !is null;
    }
}

class WriteOptions
{
private:
    leveldb_writeoptions_t _opt;

package:
    @property const const(leveldb_writeoptions_t) ptr()
    {
        return _opt;
    }

public:
    /// Create the internal option object
    this()
    {
        if((_opt = leveldb_writeoptions_create()) is null)
        {
            throw new Exception("Failed to create an read options");
        }
    }

    /// Destroy any valid option pointer
    ~this()
    {
        if(valid)
        {
            auto tmp = _opt;
            _opt = null;
            leveldb_writeoptions_destroy(tmp);
        }
    }

    @property void sync(bool val)
    {
        if(valid)
            leveldb_writeoptions_set_sync(_opt, val);
    }

    /// indicates if the option has been created
    @property bool valid()
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
