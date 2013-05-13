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
module etc.leveldb.slice;

private import etc.leveldb.exceptions;
private import std.traits : isPointer, isArray;
private import deimos.leveldb.leveldb : leveldb_free;

/**
 * Holds a pointer returned from leveldb, or passed to leveldb.
 *
 * Leveldb memory is freed on destruction.
 */
struct Slice
{
private:
    bool free = false;
    void* _ptr;
    size_t len;

package:
    this(P = void*)(void* p, size_t l, bool free)
    {
        this.free = free;
        _ptr = p;
        len = l;
    }

public:
    /// Takes reference
    this(P)(ref P p)
        if(!isArray!P && !isPointer!P)
    {
        _ptr = cast(void*)(&p);
        len = P.sizeof;
    }

    /// Takes reference
    this(P)(P* p, size_t l)
        if(!isArray!P && !isPointer!P)
    {
        _ptr = cast(void*)p;
        len = l;
    }

    /// Leveldb Slice of an array/slice
    this(A)(A[] a)
    {
        _ptr = cast(void*)a.ptr;
        len = a.length * A.sizeof;
    }

    /// Calles free on leveldb raw memory
    ~this()
    {
        if(free)
            leveldb_free(_ptr);
    }

    /// Get slice pointer
    @property
    inout(T) ptr(T)() inout
        if(isPointer!T)
    {
        return cast(inout(T))_ptr;
    }

    /// Get slice as a data type
    @property
    inout(T) as(T)() inout
        if(!isPointer!T)
    {
        static if(isArray!T)
            return  cast(inout(T))(cast(char[])(_ptr)[0..length]);
        else
        {
            if(T.sizeof > length)
                throw new LeveldbException("Casting size is larger then slice data");
            return *(cast(inout(T*))_ptr);
        }
    }

    /// length or size of slice
    @property
    size_t length() inout
    {
        return len;
    }

    /// Test is slice is valid
    @property
    bool ok() inout
    {
        return _ptr !is null;
    }

    /// Slice casting
    inout(T) opCast(T)() inout
        if(!isPointer!T)
    {
        static if(isPointer!T)
            return ptr!T;
        else
            return as!T;
    }

    static Slice Ref(T)(T t)
    {
        align(1) struct Ref{ T t; }
        return Slice(new Ref(t), T.sizeof);
    }
}


unittest
{
    auto s = "Hello";
    auto s1 = Slice(s);
    assert(s1);
    assert(s1.ok);
    assert(s1.length == 5);
    assert(s1.ptr!(const(char*)) == s.ptr);
    assert(s1.length == s.length);
    assert(s1.as!string == s);
}

unittest
{
    auto s = "Hello World";
    auto s1 = Slice(s[0..5]);
    assert(s1.ok);
    assert(s1.length == 5);
    assert(s1.ptr!(const(char*)) == s.ptr);
    assert(s1.length == s[0..5].length);
    assert(s1.as!string == s[0..5]);
}

unittest
{
    int s = 454;
    auto s1 = Slice(s);
    assert(s1.ok);
    assert(s1.length == int.sizeof);
    assert(s1.length == s.sizeof);
    assert(s1.ptr!(int*) == &s);
    assert(s1.as!int == s);
}

unittest
{
    struct Point(T)
    {
        T x, y;
    }

    auto p1 = Point!int(1, 2);
    auto s = Slice(p1);
    assert(s.ok);
    assert(s.as!(Point!int).x == 1);
    assert(s.as!(Point!int).y == 2);
    try
    {
        s.as!(Point!long);
        assert(false, "Should have thrown");
    }catch(LeveldbException e)
    {}
    catch(Exception e)
    {
        assert(false, "Should have thrown a LeveldbException");
    }
}

unittest
{
    align(1) struct Ref(T) { T t; }
    auto s1 = Slice(new Ref!int(451), 4);
    assert(s1.ok);
    assert(s1.length == int.sizeof);
    assert(s1.as!int == 451);

    /// Make a safe constant slice
    s1 = Slice.Ref(999);
    assert(s1.ok);
    assert(s1.length == int.sizeof);
    assert(s1.as!int == 999);
}
