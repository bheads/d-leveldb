module etc.leveldb.db;

private import std.string : toStringz;
private import std.conv : to;

private import
    etc.leveldb.options,
    etc.leveldb.status;

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
        auto status = open(opt, path);
        if(!status.ok)
            throw new Exception("Failed to open database at " ~ path ~ ": " ~ status.toString);
    }

    ~this()
    {
        close();
    }

    Status open(Options opt, string path)
    {
        char* errptr = null;
        close();
        _db = leveldb_open(opt.ptr, toStringz(path), &errptr);
        return Status(errptr);
    }

    void close()
    {
        if(isOpen)
        {
            auto tmp = _db;
            _db = null;
            leveldb_close(tmp);
        }
    }

    /// todo: fix default writter, exceptions
    Status put(K, V)(K key, V val, const WriteOptions opt = DefaultWriteOptions)
        if(__traits(compiles, key.ptr) && __traits(compiles, key.length) &&
            __traits(compiles, val.ptr) && __traits(compiles, val.length))
    {
        char* errptr;
        if(isOpen)
            leveldb_put(_db, opt.ptr, key.ptr, key.length, val.ptr, val.length, &errptr);
        return Status(errptr);
    }

    /// todo: fix default writter, exceptions
    Status del(K)(K key, const WriteOptions opt = DefaultWriteOptions)
        if(__traits(compiles, key.ptr) && __traits(compiles, key.length))
    {
        char* errptr;
        if(isOpen)
            leveldb_delete(_db, opt.ptr, key.ptr, key.length, &errptr);
        return Status(errptr);
    }

    Status get(K, V)(K key, out V value, const ReadOptions opt = DefaultReadOptions)
        if(__traits(compiles, key.ptr) && __traits(compiles, key.length) &&
            __traits(compiles, to!V(['a'])))
    {
        char* errptr;
        char* val;
        size_t vallen;
        if(isOpen)
        {
            val = leveldb_get(_db, opt.ptr, key.ptr, key.length, &vallen, &errptr);
            if(val !is null)
            {
                value = to!V(val[0..vallen]);
                leveldb_free(val);
            }
        }
        return Status(errptr);
    }

    Status write(const WriteBatch batch, const WriteOptions opt = DefaultWriteOptions)
    {
        char* errptr;
        if(isOpen)
            leveldb_write(_db, opt.ptr, cast(leveldb_writebatch_t)batch.ptr, &errptr);
        return Status(errptr);
    }

    @property ASnapshot getSnapshot()
    {
        return new Snapshot();
    }

    @property Iterator getIterator(const ReadOptions opt = DefaultReadOptions)
    {
        return new Iterator(opt);
    }

    @property bool isOpen()
    {
        return _db !is null;
    }

    class Snapshot : ASnapshot
    {
    private:
        leveldb_snapshot_t _snap;

    public:
        @property override const const(leveldb_snapshot_t) ptr()
        {
            return _snap;
        }

        this()
        {
            if((_snap = cast(leveldb_snapshot_t)leveldb_create_snapshot(_db)) is null)
                throw new Exception("Failed to create snapshot");
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
        @property bool valid()
        {
            return _snap !is null;
        }
    }

    class Iterator
    {
    private:
        leveldb_iterator_t _iter;

    package:
        @property const const(leveldb_iterator_t) ptr()
        {
            return _iter;
        }

    public:
        this(const ReadOptions opt = DefaultReadOptions)
        {
            if((_iter = leveldb_create_iterator(_db, opt.ptr)) is null)
                throw new Exception("Failed to create iterator");
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

        @property bool valid()
        {
            return cast(bool)leveldb_iter_valid(_iter);
        }

        @property void seek_to_first()
        {
            leveldb_iter_seek_to_first(_iter);
        }

        @property void seek_to_last()
        {
            leveldb_iter_seek_to_last(_iter);
        }

        @property void seek(K)(K key)
            if(__traits(compiles, key.ptr) && __traits(compiles, key.length))
        {
            leveldb_iter_seek(_iter, key.ptr, key.length);
        }

        @property void next()
        {
            leveldb_iter_next(_iter);
        }

        @property void prev()
        {
            leveldb_iter_prev(_iter);
        }

        @property K key(K)()
            if(__traits(compiles, to!K(['a'])))
        {
            size_t vallen;
            auto val = leveldb_iter_key(_iter, &vallen);
            scope(exit) leveldb_free(cast(void*)val);
            return to!K(val[0..vallen]);
        }

        @property V value(V)()
            if(__traits(compiles, to!V(['a'])))
        {
            size_t vallen;
            auto val = leveldb_iter_value(_iter, &vallen);
            scope(exit)leveldb_free(cast(void*)val);
            return to!V(val[0..vallen]);
        }

        @property Status status()
        {
            char* errptr;
            leveldb_iter_get_error(_iter, &errptr);
            return Status(errptr);
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

class WriteBatch
{
private:
    leveldb_writebatch_t _ptr;

package:
    @property const const(leveldb_writebatch_t) ptr()
    {
        return _ptr;
    }

private:
    this()
    {
        if((_ptr = leveldb_writebatch_create()) is null)
            throw new Exception("Failed to create write batch");
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

    @property void clear()
    {
        debug if(valid)
            leveldb_writebatch_clear(_ptr);
    }

    void put(K, V)(K key, V val)
        if(__traits(compiles, key.ptr) && __traits(compiles, key.length) &&
        __traits(compiles, val.ptr) && __traits(compiles, val.length))
    {
        leveldb_writebatch_put(_ptr, key.ptr, key.length, val.ptr, val.length);
    }

    void del(K)(K key)
        if(__traits(compiles, key.ptr) && __traits(compiles, key.length))
    {
        leveldb_writebatch_delete(_ptr, key.ptr, key.length);
    }

    void iterate(Visitor visitor)
    {
        leveldb_writebatch_iterate(_ptr, cast(void*)&visitor,
            &batchPut, &batchDel);
    }

    void iterate(void delegate(const char[] key, const char[] value) puts,
        void delegate(const char[] key) dels)
    {
        iterate(Visitor(puts, dels));
    }

    @property bool valid()
    {
        return _ptr !is null;
    }

    static struct Visitor
    {
        void delegate(const char[] key, const char[] value) puts;
        void delegate(const char[] key) dels;
    }
}

Status destoryDB(const Options opt, string path)
{
    char* errptr;
    leveldb_destroy_db(opt.ptr, toStringz(path), &errptr);
    return Status(errptr);
}

Status repairDB(const Options opt, string path)
{
    char* errptr;
    leveldb_repair_db(opt.ptr, toStringz(path), &errptr);
    return Status(errptr);
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
