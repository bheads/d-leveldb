module etc.leveldb.db;

private import std.string : toStringz;
private import std.conv : to;
private import std.traits : isArray, isPointer;

private import
    etc.leveldb.exceptions,
    etc.leveldb.options;

private import deimos.leveldb.leveldb;


class DB
{
private:
    leveldb_t _db;

public:
    this()
    {}

    this(Options opt, string path)
    {
        open(opt, path);
    }

    ~this()
    {
        close();
    }

    void open(Options opt, ref const(string) path)
    {
        // Close the connection if we are trying to reopen the db
        close();
        
        // Catch any leveldb errors
        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        _db = leveldb_open(opt.ptr, toStringz(path), &errptr);
        if(errptr) throw new LeveldbException(errptr);
        if(!_db) throw new LeveldbException(`Failed to connect to '` ~ path ~ `', unknown reason`);
    }

    @property
    void close() nothrow
    {
        if(isOpen)
        {
            auto tmp = _db;
            _db = null;
            leveldb_close(tmp);
        }
    }

    // Leveldb Put, can pass arrays or 'slices' (pointer and length)

    void put(K, V)(K key, V val, const(WriteOptions) opt = DefaultWriteOptions)
        if(isArray(K) && isArray(V))
    {
        put(key.ptr, key.length * K.sizeof, val.ptr, val.length * V.sizeof, opt);
    }

    void put(K, V)(K key, size_t keylen, V val, const(WriteOptions) opt = DefaultWriteOptions)
        if(isPointer(K) && isArray(V))
    {
        put(key, keylen, val.ptr, val.length * V.sizeof, opt);
    }

    void put(K, V)(K key, V val, size_t vallen, const(WriteOptions) opt = DefaultWriteOptions)
        if(isArray(K) && isPointer(V))
    {
        put(key.ptr, key.length * K.sizeof, val, vallen, opt);
    }

    void put(K, V)(K key, size_t keylen, V val, size_t vallen, const(WriteOptions) opt = DefaultWriteOptions)
        if(isPointer(V) && isPointer(V))
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);
        
        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        leveldb_put(_db, opt.ptr, key, keylen, val, vallen, &errptr);
        if(errptr) throw new LeveldbException(errptr);
    }

    // Delete operator

    void del(K)(K key, const(WriteOptions) opt = DefaultWriteOptions)
        if(isArray(K))
    {
        del(key.ptr, key.length * K.sizeof, opt);
    }

    void del(K)(K key, size_t keylen, const(WriteOptions) opt = DefaultWriteOptions)
        if(isPointer(K))
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);
        
        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        leveldb_delete(_db, opt.ptr, key.ptr, keylen, &errptr);
        if(errptr) throw new LeveldbException(errptr);
    }

    // Getters

    bool get(K, V)(K key, out V value, const(ReadOptions) opt = DefaultReadOptions)
        if(isArray(K) && __traits(compiles, to!V([-1, 2, 4])))
    {
        return get(key.ptr, key.length * K.sizeof, value, opt);
    }

    bool get(K, V)(K key, size_t keylen, out V value, const(ReadOptions) opt = DefaultReadOptions)
        if(isPointer(K) && __traits(compiles, to!V(['a'])))
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);

        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        size_t vallen;
        auto val = leveldb_get(_db, opt.ptr, key.ptr, keylen, &vallen, &errptr);
        if(errptr) throw new LeveldbException(errptr);
        if(val !is null)
        {
            value = to!V(val[0..vallen]);
            leveldb_free(val);
            return true;
        }
        return false;
    }

    const(CPointer) get(K)(K key, const(ReadOptions) opt = DefaultReadOptions)
        if(isArray(K))
    {
        return get(k.ptr, k.length * K.sizeof, opt);
    }

    const(CPointer) get(K)(K key, size_t keylen, const(ReadOptions) opt = DefaultReadOptions)
        if(isPointer(K))
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);

        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        size_t vallen;
        auto val = leveldb_get(_db, opt.ptr, key.ptr, keylen, &vallen, &errptr);
        if(errptr) throw new LeveldbException(errptr);
        return new CPointer(val, vallen);
    }

    // Batch write

    void write(const(WriteBatch) batch, const(WriteOptions) opt = DefaultWriteOptions)
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);

        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        leveldb_write(_db, opt.ptr, cast(leveldb_writebatch_t)batch.ptr, &errptr);
        if(errptr) throw new LeveldbException(errptr);
    }

    @property
    ASnapshot getSnapshot()
    {
        return new Snapshot();
    }

    @property
    Iterator getIterator(const(ReadOptions) opt = DefaultReadOptions)
    {
        return new Iterator(opt);
    }

    @property
    bool isOpen() inout nothrow
    {
        return _db !is null;
    }

    class Snapshot : ASnapshot
    {
    private:
        leveldb_snapshot_t _snap;

    public:
        @property 
        override inout(leveldb_snapshot_t) ptr() inout
        {
            return _snap;
        }

        this()
        {
            if((_snap = cast(leveldb_snapshot_t)leveldb_create_snapshot(_db)) is null)
                throw new LeveldbException("Failed to create snapshot");
        }

        ~this()
        {
            if(valid)
            {
                auto tmp = _snap;
                _snap = null;
                leveldb_release_snapshot(_db, tmp);
            }
        }

        /// indicates if the snapshot has been created
        @property 
        bool valid() inout
        {
            return _snap !is null;
        }
    }

    class Iterator
    {
    private:
        leveldb_iterator_t _iter;

    package:
        @property
        inout(leveldb_iterator_t) ptr() inout
        {
            return _iter;
        }

    public:
        this(const(ReadOptions) opt = DefaultReadOptions)
        {
            if((_iter = leveldb_create_iterator(_db, opt.ptr)) is null)
                throw new LeveldbException("Failed to create iterator");
            seek_to_first;
        }

        ~this()
        {
            if(_iter !is null)
            {
                auto tmp = _iter;
                _iter = null;
                leveldb_iter_destroy(tmp);
            }
        }

        @property 
        bool valid() inout
        {
            return cast(bool)leveldb_iter_valid(_iter);
        }

        @property 
        void seek_to_first()
        {
            leveldb_iter_seek_to_first(_iter);
        }

        @property 
        void seek_to_last()
        {
            leveldb_iter_seek_to_last(_iter);
        }

        @property
        void seek(K)(K key)
            if(isArray(K))
        {
            seek(key.ptr, key.length * K.sizeof);
        }

        @property
        void seek(K)(K key, size_t keylen)
            if(isPointer(K))
        {
            leveldb_iter_seek(_iter, key, keylen);
        }

        @property
        void next()
        {
            leveldb_iter_next(_iter);
        }

        @property
        void prev()
        {
            leveldb_iter_prev(_iter);
        }

        @property
        K key(K)()
            if(__traits(compiles, to!K(['a'])))
        {
            size_t vallen;
            auto val = leveldb_iter_key(_iter, &vallen);
            scope(exit) leveldb_free(cast(void*)val);
            return to!K(val[0..vallen]);
        }

        @property
        V value(V)()
            if(__traits(compiles, to!V(['a'])))
        {
            size_t vallen;
            auto val = leveldb_iter_value(_iter, &vallen);
            scope(exit)leveldb_free(cast(void*)val);
            return to!V(val[0..vallen]);
        }

        @property
        string status() inout
        {
            char* errptr = null;
            scope(exit) if(errptr) leveldb_free(cast(void*)errptr);
            leveldb_iter_get_error(_iter, &errptr);
            return to!string(errptr);
        }

        int opApply(int delegate(string) dg)
        {
            int result = 0;
            while(valid)
            {
                if((result = dg(value!string)) == 0 )
                    break;
                next;
            }
            return result;
        }

        int opApplyReverse(int delegate(string) dg)
        {
            int result = 0;
            while(valid)
            {
                if((result = dg(value!string)) == 0 )
                    break;
                prev;
            }
            return result;
        }

        int opApply(int delegate(string, string) dg)
        {
            int result = 0;
            while(valid)
            {
                if((result = dg(key!string, value!string)) == 0 )
                    break;
                next;
            }
            return result;
        }

        int opApplyReverse(int delegate(string, string) dg)
        {
            int result = 0;
            while(valid)
            {
                if((result = dg(key!string, value!string)) == 0 )
                    break;
                prev;
            }
            return result;
        }
    }
}

class CPointer
{
private:
    void* _ptr;
    size_t len;

public:
    this(P)(P* p, size_t l)
    {
        _ptr = p;
        len = l;
    }

    ~this()
    {
        leveldb_free(_ptr);
    }

    @property
    inout(T) ptr(T)() inout
        if(isPointer(T))
    {
        return _ptr;
    }

    @property
    size_t length() inout
    {
        return len;
    }

    @property
    bool ok() inout
    {
        return _ptr !is null;
    }
}

class WriteBatch
{
private:
    leveldb_writebatch_t _ptr;

package:
    @property
    inout(leveldb_writebatch_t) ptr() inout
    {
        return _ptr;
    }

private:
    this()
    {
        if((_ptr = leveldb_writebatch_create()) is null)
            throw new LeveldbException("Failed to create batch writer");
    }

    ~this()
    {
        if(valid)
        {
            auto tmp = _ptr;
            _ptr = null;
            leveldb_writebatch_destroy(tmp);
        }
    }

    @property
    void clear()
    {
        leveldb_writebatch_clear(_ptr);
    }

    void put(K, V)(K key, V val)
        if(isArray(K) && isArray(V))
    {
        put(key.ptr, key.length * K.size_t, val.ptr, val.length * V.sizeof);
    }

    void put(K, V)(K key, size_t keylen, V val)
        if(isPointer(K) && isArray(V))
    {
        put(key.ptr, key.length * K.size_t, valptr, vallen);

    }

    void put(K, V)(K key, V val, size_t vallen)
        if(isArray(K) && isPointer(V))
    {
        put(key, keylen, val.ptr, val.length * V.sizeof);
    }

    void put(K, V)(K key, size_t keylen, V val, size_t vallen)
        if(isPointer(K) && isPointer(V))
    {
        leveldb_writebatch_put(_ptr, key, keylen, val, vallen);
    }



    void del(K)(K key)
        if(isArray(K))
    {
        del(key.ptr, key.length * K.sizeof);
    }

    void del(K)(K key, size_t keylen)
    {
        leveldb_writebatch_delete(_ptr, key, keylen);
    }

    void iterate(Visitor visitor)
    {
        leveldb_writebatch_iterate(_ptr, cast(void*)&visitor,
            &batchPut, &batchDel);
    }

    void iterate(void delegate(const(char[]) key, const(char[]) value) puts,
        void delegate(const(char[]) key) dels)
    {
        iterate(Visitor(puts, dels));
    }

    @property
    bool valid() inout
    {
        return _ptr !is null;
    }

    static struct Visitor
    {
        void delegate(const(char[]) key, const(char[]) value) puts;
        void delegate(const(char[]) key) dels;
    }
}

void destoryDB(const Options opt, string path)
{
    char* errptr = null;
    scope(failure) if(errptr) leveldb_free(errptr);
    leveldb_destroy_db(opt.ptr, toStringz(path), &errptr);
    if(errptr) throw new LeveldbException(errptr);
}

void repairDB(const Options opt, string path)
{
    char* errptr = null;
    scope(failure) if(errptr) leveldb_free(errptr);

    leveldb_repair_db(opt.ptr, toStringz(path), &errptr);
    if(errptr) throw new LeveldbException(errptr);
}

private:
extern(C):

void batchPut(void* state, const char* k, size_t klen, const char* v, size_t vlen)
{
    auto visitor = cast(WriteBatch.Visitor*)state;
    visitor.puts(k[0..klen], v[0..vlen]);
}

void batchDel(void* state, const char* k, size_t klen)
{
    auto visitor = cast(WriteBatch.Visitor*)state;
    visitor.dels(k[0..klen]);
}
