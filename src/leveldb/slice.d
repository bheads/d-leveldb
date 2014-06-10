/**
 * D-LevelDB Slice
 *
 * Pointer Slice
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
module leveldb.slice;

private:
import leveldb.exceptions;
import deimos.leveldb.leveldb : leveldb_free;
import std.traits : isBasicType, isArray, isPointer, ForeachType;


package:

template isPrimitive(T) 
{
    static if(isBasicType!T || (isArray!T && isBasicType!(ForeachType!T))) {
        enum isPrimitive = true;
    } else {
        enum isPrimitive = false;
    }
}

/**
 * Holds a pointer returned from leveldb, or passed to leveldb.
 *
 * Leveldb memory is freed on destruction.
 */
struct Slice
{
private:
    void* _ptr;         // the pointer
    size_t len;         // the size of the data block
    bool free = false;  // should this block be freed by leveldb_free

    this(void* p, size_t l, bool f = false)
    {
        debug {
            import std.stdio;
            writeln("length: ", l);
        }
        _ptr = p;
        len = l;
        free = f;
    }

public:

    /// Calles free on leveldb raw memory
    ~this()
    {
        if(free) {
            leveldb_free(_ptr);
        }
    }


    /// length or size of slice
    @property
    size_t size() inout pure nothrow
    {
        return len;
    }
    
    /// Get slice pointer
    @property
    const(char)* cptr() inout pure nothrow
    {
        return cast(const(char)*)_ptr;
    }

    @property
    inout(T) ptr(T)() inout pure nothrow
        if(isPointer!T)
    {
        return cast(inout(T))_ptr;
    }

    /// Test is slice is valid
    @property
    bool ok() inout pure nothrow
    {
        return _ptr !is null;
    }

    /// Get slice as a data type
    alias to = as;
    @property
    auto as(T)()
        if(isPrimitive!T)
    {
        static if(isArray!T) {
            return  cast(T)(_ptr[0..size]).dup;
        } else {
            return *cast(T*)_ptr;
        }
    }

    /// Slice casting
    inout(T) opCast(T)() inout
    {
        return as!T;
    }

    // helper
    auto static make(T)(T t) 
        if(isPrimitive!T) 
    out(slice){
            static if(isArray!T) {
                assert(slice._ptr == t.ptr);
            } else {
                assert(slice._ptr == &t);
                assert(slice.as!T == t);
            }
    } body {
        debug {
            import std.stdio;
            writeln("T: ", T.stringof, " t: ", t);
        }
        static if(isArray!T) {
            return Slice(cast(void*)t.ptr, t.length * (ForeachType!T).sizeof);
        } else {
            return Slice(cast(void*)&t, T.sizeof);
        }
    }

    // helper
    auto static make(T)(T t, size_t size, bool free = false) if(isPointer!T) {
        debug {
            import std.stdio;
            writeln("T", T.stringof, " size: ", size, " free: ", free);
        }
        return Slice(cast(void*)t, size, free);
    }
}

