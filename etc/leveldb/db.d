/**
 * D-LevelDB DateBase Object
 *
 * This is the main database object.  This object connects to a Leveldb database.
 *
 * Copyright: Copyright © 2013 Byron Heads
 * License: <a href="http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors: Byron Heads
 *
 * Example:
 ---
 auto opt = new Options;
 opt.create_if_missing = true;
 auto db = new DB(opt, "/my/db/");
 db.put(Slice("PI"), Slice.Ref!double(3.14));
 double pi;
 enforce(db.get(Slice("PI"), pi));
 assert(pi == 3.14);
 ---
 * Todo: Add searching and transactions
*/
/*          Copyright  © 2013 Byron Heads
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
module etc.leveldb.db;

private import std.string : toStringz;
private import std.conv : to;
private import std.traits : isPointer, isArray;
private import std.c.string : memcpy;
private import core.memory : GC;

// Use the temp space for unittesting
version(unittest)
{
    private import std.file : 
        tempDir, mkdirRecurse, rmdirRecurse ;
    private import std.stdio;

    const __gshared const(string) tempPath;
    static this()
    {
        tempPath = tempDir() ~ `unittest_d_leveldb/`;
        try
        {
            writeln("Temp Path: ", tempPath);
            mkdirRecurse(tempPath);
        } catch(Exception e) {}
    }

    static ~this()
    {
        try
        {
            rmdirRecurse(tempPath);
        } catch(Exception e) {}
    }
}

private import
    etc.leveldb.exceptions,
    etc.leveldb.slice,
    etc.leveldb.writebatch,
    etc.leveldb.options;

private import deimos.leveldb.leveldb;

/**
 * LevelDB DB
 * 
 * Throws: LeveldbException on errors
 */
class DB
{
private:
    leveldb_t _db;  /// Internal LevelDB Pointer

public:
    /** Create a new unconnected DB */
    this()
    {}

    /**
     * Opens a new connection to a levelDB database on creation
     * 
     * Params:
     *      opt = LevelDB Options, sets the db options
     *      path = path to the leveldb files, each DB needs its own path
     * Throws: LeveldbException
     */
    this(Options opt, string path)
    {
        open(opt, path);
    }

    /** Force database to close on destruction, cleans up library memory */
    ~this()
    {
        close();
    }

    /**
     * Opens a new connection to a levelDB database
     * 
     * Params:
     *      opt = LevelDB Options, sets the db options
     *      path = path to the leveldb files, each DB needs its own path
     * Throws: LeveldbException
     */
    void open(Options opt, string path)
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

    /**
     * Close DB connection, also frees _db pointer in leveldb lib
     */
    @property
    void close() nothrow
    {
        if(isOpen)
        {
            leveldb_close(_db);
            _db = null;
        }
    }

    /**
     * Inserts/Updates a given value at a given key.
     *
     * Example:
     ---
     auto opt = new Options;
     opt.create_if_missing = true;
     auto db = new DB(opt, "/my/db/");
     db.put(Slice("User1"), "John Doe");
     ---
     * Throws: LeveldbException
     */
    void put(K, V)(K key, V val, const(WriteOptions) opt = DefaultWriteOptions)
    {
        static if(__traits(isSame, K, Slice) && __traits(isSame, V, Slice))
            put_raw(key.ptr!(const(char*)), key.length, val.ptr!(const(char*)), val.length, opt);
        else static if(!__traits(isSame, K, Slice) && __traits(isSame, V, Slice))
            put_raw(cast(const(char*))pointer(key), size(key), val.ptr!(const(char*)), val.length, opt);
        else static if(__traits(isSame, K, Slice) && !__traits(isSame, V, Slice))
            put_raw(key.ptr!(const(char*)), key.length, cast(const(char*))pointer(val), size(val), opt);
        else
            put_raw(cast(const(char*))pointer(key), size(key), cast(const(char*))pointer(val), size(val), opt);
    }

    /**
     * Inserts/Updates a given value at a given key.  This is the real call to leveldb
     *
     * Throws: LeveldbException
     */
    private
    void put_raw(const(char*) key, size_t keylen, const(char*) val, size_t vallen, const(WriteOptions) opt)
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);

        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        leveldb_put(_db, opt.ptr, key, keylen, val, vallen, &errptr);
        if(errptr) throw new LeveldbException(errptr);
    }

    /**
     * Deletes a key from the db
     *
     * Example:
     ---
     auto opt = new Options;
     opt.create_if_missing = true;
     auto db = new DB(opt, "/my/db/");
     db.put("User1", Slice("John Doe"));
     db.del("User1");
     ---
     * Throws: LeveldbException
     */
    void del(T)(T key, const(WriteOptions) opt = DefaultWriteOptions)
    {
        static if(__traits(isSame, T, Slice))
            del_raw(key.ptr!(const(char*)), key.length, opt);
        else
            del_raw(cast(const(char*))pointer(key), size(key), opt);
    }

    /**
     * Deletes a key from the db.  Calles leveldb_delete
     *
     * Throws: LeveldbException
     */
    private
    void del_raw(const(char*) key, size_t keylen, const(WriteOptions) opt)
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);
        
        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        leveldb_delete(_db, opt.ptr, key, keylen, &errptr);
        if(errptr) throw new LeveldbException(errptr);
    }

    /**
     * Gets an entry from the DB
     *
     * Only accepts an array for the key.
     * V must be convertable from char array.
     *
     * Example:
     ---
     auto opt = new Options;
     opt.create_if_missing = true;
     auto db = new DB(opt, "/my/db/");
     db.put("User1", Slice("John Doe"));
     string name;
     enforce(db.get("User1", name));
     assert(name == "John Doe");
     ---
     * Throws: LeveldbException
     * Returns: true if the key was found in the DB
     */
    bool get(T, V)(T key, out V value, const(ReadOptions) opt = DefaultReadOptions)
    {
        static if(__traits(isSame, T, Slice))
            return get_raw(key.ptr!(const(char*)), key.length, value, opt);
        else
            return get_raw(cast(const(char*))pointer(key), size(key), value, opt);
    }

    /**
     * Gets an entry from the DB
     *
     * Calls leveldb_get
     * V must be convertable from char array.
     *
     * Throws: LeveldbException
     * Returns: true if the key was found in the DB
     */
    bool get_raw(V)(const(char*) key, size_t keylen, out V value, const(ReadOptions) opt)
        if(!is(V == const(ReadOptions)))
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);

        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        size_t vallen;
        auto val = leveldb_get(_db, opt.ptr, key, keylen, &vallen, &errptr);
        scope(exit) if(val) leveldb_free(val);
        if(errptr) throw new LeveldbException(errptr);
        if(val !is null)
        {
            static if(is(V == interface)) assert(0);
            static if(isArray!V) value = cast(V)(val[0..vallen]);
            else static if(__traits(compiles, value.dup)) value = *(cast(V*)val).dup;
            else static if(__traits(compiles, value = new V(*(cast(const(V*))val)))) value = new V(*(cast(const(V*))val));
            else
            {
                static if(is(V == class))
                if(value is null) value = *cast(V*)GC.malloc(V.sizeof);
                memcpy(cast(void*)&value, cast(const(void*))val, V.sizeof);
            }
            return true;
        }
        return false;
    }

    /**
     * Gets an entry from the DB as a Slice.
     *
     * Example:
     ---
     auto opt = new Options;
     opt.create_if_missing = true;
     auto db = new DB(opt, "/my/db/");
     auto uuid = UUID("8AB3060E-2cba-4f23-b74c-b52db3bdfb46");
     db.put("My UUID", uuid.data);
     auto name = db.get_slice("My UUID");
     assert(name.as!UUID == uuid);
     ---
     * Throws: LeveldbException
     * Returns: A Slice struct, this holds the returned pointer and size
     * Slice will safely clean up the result
     */
    auto get_slice(T)(T key, const(ReadOptions) opt = DefaultReadOptions)
    {
        static if(__traits(isSame, T, Slice))
            return get_slice_raw(key.ptr!(const(char*)), key.length, opt);
        else
            return get_slice_raw(cast(const(char*))pointer(key), size(key), opt);
    }

    /**
     * Gets an entry from the DB as a Slice.
     *
     * Calles leveldb_get
     *
     * Example:
     * Throws: LeveldbException
     * Returns: A Slice struct, this holds the returned pointer and size
     * Slice will safely clean up the result
     */
    private
    auto get_slice_raw(const(char*) key, size_t keylen, const(ReadOptions) opt)
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);

        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        size_t vallen;
        void* val = leveldb_get(_db, opt.ptr, key, keylen, &vallen, &errptr);
        scope(failure) if(val) leveldb_free(val);
        if(errptr) throw new LeveldbException(errptr);
        return Slice(val, vallen, true);
    }

    /**
     * Sublmits a BatchWrite to the DB.
     *
     * Used to do batch writes.
     *
     * Example:
     ---
     auto opt = new Options;
     opt.create_if_missing = true;
     auto db = new DB(opt, "/my/db/");
     // Unsafe banking example
     auto batch = new WriteBatch;

     double joe = db.get_slice(Slice("Joe")).as!double;
     double sally = db.get_slice(Slice("Sally")).as!double;

     joe -= 10.00;
     sally += 10.00;
     if(joe < 0.0)
        joe -= 30.00; // overdraft fee

     // submit the put in a single update
     batch.put(Slice("Joe"), Slice(joe));
     batch.put(Slice("Sally"), Slice(sally));
     db.write(batch);
     ---
     * Throws: LeveldbException
     */
    void write(const(WriteBatch) batch, const(WriteOptions) opt = DefaultWriteOptions)
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);

        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        leveldb_write(_db, opt.ptr, cast(leveldb_writebatch_t)batch.ptr, &errptr);
        if(errptr) throw new LeveldbException(errptr);
    }

    /** 
     * Returns a readonly snapshot
     *
     * Throws: LeveldbException
     */
    @property
    ASnapshot snapshot()
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);
        return new Snapshot();
    }

    /* 
     * Returns a database iterator
     *
     * Throws: LeveldbException
     */
    @property
    Iterator iterator(const(ReadOptions) opt = DefaultReadOptions)
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);
        return new Iterator(opt);
    }

    /**
     * Tests if the database is open
     *
     * Returns: true if there is an open database
     */
    @property
    bool isOpen() inout nothrow
    {
        return _db !is null;
    }

    /**
     * Short cut iterator, treate the db like an iterator
     */
    int opApply(int delegate(Slice) dg)
    {
        return iterator.opApply(dg);
    }

    int opApplyReverse(int delegate(Slice) dg)
    {
        return iterator.opApplyReverse(dg);
    }

    int opApply(int delegate(Slice, Slice) dg)
    {
        return iterator.opApply(dg);
    }

    int opApplyReverse(int delegate(Slice, Slice) dg)
    {
        return iterator.opApplyReverse(dg);
    }

    /**
     * DB Snapshot
     *
     * Snapshots can be applied to ReadOptions.  Created from a DB object
     *
     * Example:
     ---
     auto opt = new Options;
     opt.create_if_missing = true;
     auto db = new DB(opt, "/my/db/")

     auto snap = db.snapshot;
     db.put(Slice("Future"), Slice("Stuff"));

     auto ro = new ReadOptions;
     ro.snapshot(snap);

     string str;
     assert(db.get(Slice("Future"), str));
     assert(!db.get(Slice("Future"), str, ro));
     ---
     * Throws: LeveldbException
     */
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

        /** Cleanup snapshot memory */
        ~this()
        {
            if(valid)
            {
                leveldb_release_snapshot(_db, _snap);
                _snap = null;
            }
        }

        /// test if the snapshot has been created
        @property 
        bool valid() inout
        {
            return _snap !is null;
        }
    } // SnapShot

    /**
     * DB Iterator
     *
     * Can iterate the db
     *
     * Example:
     ---
     auto opt = new Options;
     opt.create_if_missing = true;
     auto db = new DB(opt, "/my/db/")

     auto it = db.iterator;
     foreach(Slice key, Slice value; it)
     {
        writeln(key.as!string, " - ", value.as!string);
     }
    
     ---
     * Throws: LeveldbException
     */
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
        }

        ~this()
        {
            if(ok)
            {
                leveldb_iter_destroy(_iter);
                _iter = null;
            }
        }

        /// Iterator created
        @property
        bool ok() inout
        {
            return _iter !is null;
        }

        /// Iterator has more data to read
        @property
        bool valid() inout
        {
            return cast(bool)leveldb_iter_valid(_iter);
        }

	@property
	bool empty() inout
	{
		return !valid;
	}

        /// Seek to front of data
        @property 
        void seek_to_first()
        {
            leveldb_iter_seek_to_first(_iter);
        }

        /// Seek to end of data
        @property 
        void seek_to_last()
        {
            leveldb_iter_seek_to_last(_iter);
        }

        /// Seek to given slice.
        @property
        void seek(T)(T key)
        {
            static if(__traits(isSame, T, Slice))
                leveldb_iter_seek(_iter, key.ptr!(const(char*)), key.length);
            else
                leveldb_iter_seek(_iter, cast(const(char*))pointer(key), size(key));
        }

        /// Move to next item
        @property
        void next()
        {
            leveldb_iter_next(_iter);
        }

	alias next popFront;

        /// Move to previous item
        @property
        void prev()
        {
            leveldb_iter_prev(_iter);
        }

        /// Return the current key
        @property
        Slice key()
        {
            debug if(!valid) throw new LeveldbException("Accessing invalid iterator");
            size_t vallen;
            void* val = cast(void*)leveldb_iter_key(_iter, &vallen);
            scope(failure) if(val) leveldb_free(val);
            return Slice(val, vallen, false);
        }

        /// Return the current value
        @property
        auto value()
        {
            debug if(!valid) throw new LeveldbException("Accessing invalid iterator");
            size_t vallen;
            void* val = cast(void*)leveldb_iter_value(_iter, &vallen);
            scope(failure) if(val) leveldb_free(val);
            return Slice(val, vallen, false);
        }

	/// return the front of the iterator
	@property
	auto front()
	{
		return [key, value];
	}

        /// Gets the current error status of the iterator
        @property
        string status() inout
        {
            char* errptr = null;
            scope(exit) if(errptr) leveldb_free(cast(void*)errptr);
            leveldb_iter_get_error(_iter, &errptr);
            return to!string(errptr);
        }

        /// For each on iterator
        int opApply(int delegate(Slice) dg)
        {
            int result = 0;
            for(seek_to_first; valid; next)
            {
                result = dg(value);
                if(result) return result;
            }
            return result;
        }

        int opApplyReverse(int delegate(Slice) dg)
        {
            int result = 0;
            for(seek_to_last; valid; prev)
            {
                result = dg(value);
                if(result) return result;
            }
            return result;
        }

        int opApply(int delegate(Slice, Slice) dg)
        {
            int result = 0;
            for(seek_to_first; valid; next)
            {
                result = dg(key, value);
                if(result) return result;
            }
            return result;
        }

        int opApplyReverse(int delegate(Slice, Slice) dg)
        {
            int result = 0;
            for(seek_to_last; valid; prev)
            {
                result = dg(key, value);
                if(result) return result;
            }
            return result;
        }
    } //Iterator

    /**
     * Destory/delete a non-locked leveldb
     */
    static void destroyDB(const Options opt, string path)
    {
        char* errptr = null;
        scope(exit) if(errptr) leveldb_free(errptr);
        leveldb_destroy_db(opt.ptr, toStringz(path), &errptr);
        if(errptr) throw new LeveldbException(errptr);
    }

    /**
     * Attempt to repair a non-locked leveldb
     */

    static void repairDB(const Options opt, string path)
    {
        char* errptr = null;
        scope(exit) if(errptr) leveldb_free(errptr);

        leveldb_repair_db(opt.ptr, toStringz(path), &errptr);
        if(errptr) throw new LeveldbException(errptr);
    }
} // class DB


// Basic open, write string close, open get string, del string, get it
unittest
{
    auto opt = new Options;
    opt.create_if_missing = true;
    auto db = new DB(opt, tempPath ~ `s1`);
    assert(db.isOpen);
    db.put(Slice("Hello"), Slice("World"));
    db.close;
    assert(!db.isOpen);
    db.open(opt, tempPath ~ `s1`);
    assert(db.isOpen);
    string ret;
    assert(db.get(Slice("Hello"), ret));
    assert(ret == "World");
    db.del("Hello");
    assert(!db.get(Slice("Hello"), ret));
    assert(ret != "World");
    destroy(db); // force destructor to be called
    db.destroyDB(opt, tempPath ~ `s1`);
}

unittest
{
    auto opt = new Options;
    opt.create_if_missing = true;
    auto db = new DB(opt, tempPath ~ `s1`);
    assert(db.isOpen);
    db.put("Hello", "World");
    db.close;
    assert(!db.isOpen);
    db.open(opt, tempPath ~ `s1`);
    assert(db.isOpen);
    string ret;
    assert(db.get(Slice("Hello"), ret));
    assert(ret == "World");
    db.del(Slice("Hello"));
    assert(!db.get(Slice("Hello"), ret));
    assert(ret != "World");
    destroy(db); // force destructor to be called
}

// Test raw get
unittest
{
    import std.math;
    auto opt = new Options;
    opt.create_if_missing = true;
    auto db = new DB(opt, tempPath ~ `s1`);
    auto pi = PI;
    db.put(Slice("PI"), Slice(pi));
    assert(db.get(Slice("PI"), pi));
    assert(pi == PI);
    assert(!db.get_slice("PI2").ok);
    auto pi2 = db.get_slice(Slice("PI"));
    assert(pi2.ok);
    assert(pi2.length == pi.sizeof);
    assert(pi2.as!real == pi);
}

// Test raw get
unittest
{
    auto opt = new Options;
    opt.create_if_missing = true;
    auto db = new DB(opt, tempPath ~ `s4`);
    db.put(Slice("SCORE"), Slice.Ref(234L));
    long pi;
    assert(db.get("SCORE", pi));
    assert(pi == 234);
    assert(!db.get_slice("SCORE2").ok);
    auto pi2 = db.get_slice(Slice("SCORE"));
    assert(pi2.ok);
    assert(pi2.length == pi.sizeof);
    assert(pi2.as!long == 234L);
}

// test structs as key and value
unittest
{
    struct Point
    {
        double x, y;
    }

    import std.uuid;
    auto uuid = randomUUID();

    auto opt = new Options;
    opt.create_if_missing = true;
    auto db = new DB(opt, tempPath ~ `s2`);
    auto p = Point(55, 44);
    Point p2;
    db.put(Slice(uuid.data), Slice(p));
    auto o1 = db.get_slice(Slice(uuid.data));
    assert(o1.as!Point.x == p.x);
    assert(o1.as!Point.y == p.y);
    assert(db.get(Slice(uuid.data), p2));
    auto o2 = db.get_slice(uuid.data);
    db.del(uuid.data);
    GC.collect();
    assert(p2.x == p.x);
    assert(p2.y == p.y);
    assert(!db.get(uuid.data, p2));
}

unittest
{
    auto opt = new Options;
    opt.create_if_missing = true;
    auto db = new DB(opt, tempPath ~ `wb1`);

    db.put(Slice("Joe"), Slice.Ref(25));
    db.put(Slice("Sally"), Slice.Ref(905));
    assert(db.get_slice("Joe").as!int == 25);
    assert(db.get_slice(Slice("Sally")).as!int == 905);

    auto joe = db.get_slice(Slice("Joe")).as!int - 10;
    auto sally = db.get_slice(Slice("Sally")).as!int + 10;

    auto wb = new WriteBatch();
    wb.put(Slice("Joe"), joe);
    wb.put("Sally", Slice(sally));
    assert(db.get_slice(Slice("Joe")).as!int == 25);
    assert(db.get_slice("Sally").as!int == 905);
    wb.clear;
    db.write(wb);
    assert(db.get_slice(Slice("Joe")).as!int == 25);
    assert(db.get_slice("Sally").as!int == 905);
    wb.put("Joe", joe);
    wb.put(Slice("Sally"), Slice(sally));
    db.write(wb);
    assert(db.get_slice("Joe").as!int == joe);
    assert(db.get_slice("Sally").as!int == sally);
}

unittest
{
    auto opt = new Options;
    opt.create_if_missing = true;
    auto db = new DB(opt, tempPath ~ `wb2`);

    db.put("A", 1);
    db.put("B", 1);
    assert(db.get_slice("A").ok);
    assert(db.get_slice("B").ok);
    auto wb = new WriteBatch();
    wb.del("A");
    assert(db.get_slice("A").ok);
    assert(db.get_slice("B").ok);
    db.write(wb);
    assert(!db.get_slice("A").ok);
    assert(db.get_slice("B").ok);
    db.put("A", 1);
    wb.clear;
    wb.del(Slice("B"));
    assert(db.get_slice("A").ok);
    assert(db.get_slice("B").ok);
    db.write(wb);
    assert(db.get_slice("A").ok);
    assert(!db.get_slice("B").ok);
}

unittest
{
    auto opt = new Options;
    opt.create_if_missing = true;
    DB.destroyDB(opt, tempPath ~ `ss1`);
    auto db = new DB(opt, tempPath ~ `ss1`);
    auto snap = db.snapshot;
    assert(snap);
    db.put(Slice("Future"), "Stuff");
    auto ro = new ReadOptions;
    ro.snapshot(snap);
    string str;
    assert(db.get(Slice("Future"), str));
    assert(str == "Stuff");
    assert(!db.get("Future", str, ro));
    assert(str == "");
    assert(db.get("Future", str));
    snap = db.snapshot;
    ro.snapshot(snap);
    assert(db.get("Future", str, ro));
}

unittest
{
    auto opt = new Options;
    opt.create_if_missing = true;
    DB.destroyDB(opt, tempPath ~ `it1/`);
    auto db = new DB(opt, tempPath ~ `it1/`);
    db.put(Slice("Hello"), Slice("World"));

    auto it = db.iterator;
    foreach(Slice key, Slice value; it)
    {
        assert(key.as!string == "Hello");
        assert(value.as!string == "World");
    }
    foreach_reverse(Slice key, Slice value; it)
    {
        assert(key.as!string == "Hello");
        assert(value.as!string == "World");
    }

    db.put(1, Slice.Ref(1));
    it = db.iterator;
    for(it.seek(Slice.Ref(1)); it.valid; it.next)
    {
        assert(it.key.ok);
        assert(it.value.ok);
    }

    it = db.iterator;
    for(it.seek(1); it.valid; it.next)
    {
        assert(it.key.ok);
        assert(it.value.ok);
    }
}

unittest
{
    auto opt = new Options;
    opt.create_if_missing = true;
    DB.destroyDB(opt, tempPath ~ `it2`);
    auto db = new DB(opt, tempPath ~ `it2/`);
    assert(db.isOpen);
    foreach(int i; 1..10)
    {
        db.put(i, i*2);
    }
    auto it = db.iterator;
    
    foreach(Slice key, Slice value; it)
    {
        assert(value.as!int == key.as!int * 2);
    }
    foreach_reverse(Slice key, Slice value; it)
    {
        assert(value.as!int == key.as!int * 2);
    }
}

unittest
{
    auto opt = new Options;
    opt.create_if_missing = true;
    DB.destroyDB(opt, tempPath ~ `it3/`);
    auto db = new DB(opt, tempPath ~ `it3/`);
    db.put("Hello", "World");

    foreach(Slice key, Slice value; db)
    {
        assert(key.as!string == "Hello");
        assert(value.as!string == "World");
    }
    foreach_reverse(Slice key, Slice value; db)
    {
        assert(key.as!string == "Hello");
        assert(value.as!string == "World");
    }
}

unittest
{
    auto opt = new Options;
    opt.create_if_missing = true;
    DB.destroyDB(opt, tempPath ~ `it4`);
    auto db = new DB(opt, tempPath ~ `it4/`);
    assert(db.isOpen);
    foreach(int i; 1..10)
    {
        db.put(Slice(i), i*2);
    }
    foreach(Slice key, Slice value; db)
    {
        assert(value.as!int == key.as!int * 2);
    }
    foreach_reverse(Slice key, Slice value; db)
    {
        assert(value.as!int == key.as!int * 2);
    }
}

