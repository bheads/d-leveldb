module leveldb.db;
/**
 * D-LevelDB DateBase Object
 *
 * This is the main database object.  This object connects to a Leveldb database.
 *
 * Copyright: Copyright © 2013 Byron Heads
 * License: <a href="http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors: Byron Heads
 *
 * todo: add examples
*/

/**
 *          Copyright  © 2013 Byron Heads
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */

private:
import deimos.leveldb.leveldb, 
    leveldb.exceptions, 
    leveldb.slice, 
    leveldb.writebatch, 
    leveldb.options;

import std.string : toStringz;
import std.traits;

enum isPrimitive(T) = (isSomeString!T || isNumeric!T || isBoolean!T || isSomeChar!T);


const(char*) ptr(T)(ref T t)
    if(isNumeric!T || isBoolean!T || isSomeChar!T)
{
    return cast(const(char*))&t;
}

public:

/**
 * LevelDB DB
 * 
 * Throws: LeveldbException on errors
 */
struct DB(alias pack, alias unpack)
{
private:
    leveldb_t _db;  /// Internal LevelDB Pointer

public:
    @disable this(); // removing default empty constructor
    @disable this(this); // removing default empty constructor

    /**
     * Opens a new connection to a levelDB database on creation
     * 
     * Params:
     *      path = path to the leveldb files, DBs are folders that contain db files
     *      opt = LevelDB Options, sets the db options
     * Throws: LeveldbException
     */
    this(string path, Options opt) {
        open(path, opt);
    }

    /** Force database to close on destruction, cleans up library memory */
    ~this() {
        close();
    }

    /**
     * Opens a new connection to a levelDB database
     * 
     * Params:
     *      path = path to the leveldb files, each DB needs its own path
     *      opt = LevelDB Options, sets the db options
     * Throws: LeveldbException
     */
    @property final 
    void open(string path, Options opt) {
        // Close the connection if we are trying to reopen the db
        close();

        char* errptr = null;
        scope(exit) if(errptr !is null) leveldb_free(errptr);

        _db = leveldb_open(opt.ptr, toStringz(path), &errptr);

        dbEnforce(errptr is null);
        dbEnforce(_db !is null, `Failed to connect to '` ~ path ~ `', unknown reason`);
    }

    /**
     * Close DB connection, also frees _db pointer in leveldb lib
     */
    @property final
    ref auto close() nothrow {
        if(isOpen) {
            leveldb_close(_db);
            _db = null;
        }
        return this;
    }

    @property final
    ref auto del(K)(in K key, const(WriteOptions) opt = DefaultWriteOptions)
    {
        dbEnforce(isOpen, "Not connected to a db");
        
        char* errptr = null;
        scope(exit) if(errptr !is null) leveldb_free(errptr);

        static if(isPrimitive!K) {
            leveldb_delete(_db, opt.ptr, key.ptr, K.sizeof, &errptr);
        } else {
            const(ubyte)[] keyBuf = pack!K(key);
            leveldb_delete(_db, opt.ptr, keyBuf.ptr, keyBuf.length, &errptr);
        }

        dbEnforce(!errptr);
        return this;
    }

    final
    ref auto put(K, V)(in K key, in V val, const(WriteOptions) opt = DefaultWriteOptions) {
        dbEnforce(isOpen, "Not connected to a db");

        char* errptr = null;
        scope(exit) if(errptr !is null) leveldb_free(errptr);

        static if(isPrimitive!K && isPrimitive!V) {
            leveldb_put(_db, opt.ptr, key.ptr, K.sizeof, val.ptr, V.sizeof, &errptr);
        } else static if (isPrimitive!K) {
            const(ubyte)[] valBuf = pack!V(val);
            leveldb_put(_db, opt.ptr, key.ptr, K.sizeof, valBuf.ptr, valBuf.length, &errptr);
        } else static if (isPrimitive!V) {
            const(ubyte)[] keyBuf = pack!K(key);
            leveldb_put(_db, opt.ptr, keyBuf.ptr, keyBuf.length, val.ptr, V.sizeof, &errptr);
        } else {
            const(ubyte)[] keyBuf = pack!K(key);
            const(ubyte)[] valBuf = pack!V(val);
            leveldb_put(_db, opt.ptr, keyBuf.ptr, keyBuf.length, valBuf.ptr, valBuf.length, &errptr);
        }

        dbEnforce(!errptr);
        return this;
    }

    final
    V find(K, V)(in K key, lazy V def, const(ReadOptions) opt = DefaultReadOptions) {
        dbEnforce(isOpen, "Not connected to a db");
        
        char* errptr = null;
        scope(exit) if(errptr !is null) leveldb_free(errptr);

        size_t vallen; // size of the return slice
        char* valret; // return pointer
        scope(exit) if(valret !is null) leveldb_free(valret); // make sure we clean this up

        static if(isPrimitive!K) {
            auto valptr = leveldb_get(_db, opt.ptr, key.ptr, K.sizeof, &vallen, &errptr);
        } else {
            const(ubyte)[] keyBuf = pack!K(key);
            auto valret = leveldb_get(_db, opt.ptr, keyBuf.ptr, keyBuf.length, &vallen, &errptr);
        }

        dbEnforce(!errptr);

        // Not in db return default
        if(valret is null) {
            return def;
        }

        static if(isSomeString!V) {
            return cast(V)(cast(char[])(valret)[0..vallen]).dup;
        } else static if(isPrimitive!V) {
            return *(cast(V*)valret);
        } else {
            return unpack!V(cast(ubyte[])valret);
        }
    }


    /+
    /**
     * finds an entry in the db or returns the default value
     * Example
     ---
     auto opt = new Options;
     opt.create_if_missing = true;
     auto db = new DB(opt, "/my/db/");
     db.put("user_1245_name", "John Smith);
     assert(db.find("user_1245_name", "") == "John Smith");
     ---
     * Throws: LeveldbException
     */
    V find(K, V)(in K key, V def, const(ReadOptions) opt = DefaultReadOptions)
        if(!is(V == interface))
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);

        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        size_t vallen;
        auto valptr = leveldb_get(_db, opt.ptr, key._lib_obj_ptr__, key._lib_obj_size__, &vallen, &errptr);
        scope(exit) if(valptr !is null) leveldb_free(valptr);
        if(errptr) throw new LeveldbException(errptr);
        if(valptr is null) return def;

        static if(isSomeString!V || isArray!V)
	{
            return cast(V)(cast(char[])(valptr)[0..vallen]).dup;
	}
        else static if(is(V == class))
        {
            if(typeid(V).sizeof > vallen)
                throw new LeveldbException("Assignment size is larger then data size");
            return *(cast(V*)valptr).dup;
        }
        else
        {
            if(V.sizeof > vallen)
                throw new LeveldbException("Assignment size is larger then slice data size");
            return *(cast(V*)valptr);
        }
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
    bool get(T, V)(in T key, out V value, const(ReadOptions) opt = DefaultReadOptions)
        if(!is(V == interface))
    {
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);

        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        size_t vallen;
        auto valptr = leveldb_get(_db, opt.ptr, key._lib_obj_ptr__, key._lib_obj_size__, &vallen, &errptr);
        scope(exit) if(valptr !is null) leveldb_free(valptr);
        if(errptr) throw new LeveldbException(errptr);
        if(valptr is null) return false;

        static if(isSomeString!V || isArray!V)
	{
            value = cast(V)(cast(char[])(valptr)[0..vallen]).dup;
	}
        else static if(is(V == class))
        {
            if(typeid(V).sizeof > vallen)
                throw new LeveldbException("Assignment size is larger then data size");
            value = *(cast(V*)valptr).dup;
        }
        else
        {
            if(V.sizeof > vallen)
                throw new LeveldbException("Assignment size is larger then slice data size");
            value = *(cast(V*)valptr);
        }

        return true;
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
        if(!isOpen) throw new LeveldbException(`Not connected to a valid db`);

        char* errptr = null;
        scope(failure) if(errptr) leveldb_free(errptr);

        size_t vallen;
        void* val = leveldb_get(_db, opt.ptr, key._lib_obj_ptr__, key._lib_obj_size__, &vallen, &errptr);
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
            leveldb_iter_seek(_iter, key._lib_obj_ptr__, key._lib_obj_size__);
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
    +/

    /**
     * Tests if the database is open
     *
     * Returns: true if there is an open database
     */
    @property final
    bool isOpen() inout pure nothrow
    {
        return _db !is null;
    }
} // class DB


// Basic open, and close
unittest {
    import std.file, std.path;
    auto opt = new Options;
    opt.create_if_missing = true;
    auto db = new DB!(null, null)(buildNormalizedPath(tempDir, "d-leveldb_unittest.db"), opt);
    assert(db.isOpen);
    assert(!db.close.isOpen);
}

// Putting in basic values
unittest {
    import std.file, std.path, std.stdio;
    auto opt = new Options;
    opt.create_if_missing = true;
    auto db = new DB!(null, null)(buildNormalizedPath(tempDir, "d-leveldb_unittest.db"), opt);
    assert(db.isOpen);
    db.put("testing", 123).put(123, "blah");
    writeln(db.find("testing", 0));
    assert(db.find(123, "") == "blah");
    db.del(123);
    assert(db.find(123, "") == "");
    assert(db.find("testing", 5566) == 123);
    assert(!db.close.isOpen);
}