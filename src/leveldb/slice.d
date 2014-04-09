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

private import leveldb.exceptions;
private import std.traits : isArray, isStaticArray, isDynamicArray,
                            isPointer, isBasicType, ForeachType, isSomeString;
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
    /// Used by DB class to hold leveldb raw pointers
    this(P = void*)(void* p, size_t l, bool free)
    {
        this.free = free;
        _ptr = p;
        len = l;
    }

public:
    /// Takes reference
    this(P)(ref P p)
    {
        this(p._lib_obj_ptr__, p._lib_obj_size__);
    }

    this(P)(in P p)
        if(!__traits(isRef, p))
    {
        this(p._lib_obj_ptr__, p._lib_obj_size__);
    }

    /// Takes reference
    this(P)(P p, size_t l)
        if(isPointer!P)
    {
        _ptr = cast(void*)p;
        len = l;
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

    alias ptr!(const(char*)) _lib_obj_ptr__;

    /// Get slice as a data type
    @property
    inout(T) as(T)() inout
        if(!isPointer!T && __traits(compiles, *(cast(inout(T*))_ptr)))
    {
        static if(isArray!T)
            return  cast(inout(T))(cast(char[])(_ptr)[0..length]);
        else static if(is(T == class))
        {
            if(typeid(T).sizeof > length)
                throw new LeveldbException("Casting size is larger then slice data");
            return *(cast(inout(T*))_ptr);
        }
        else
        {
            if(T.sizeof > length)
                throw new LeveldbException("Casting size is larger then slice data");
            return *(cast(inout(T*))_ptr);
        }
    }

    alias as to;

    /// length or size of slice
    @property
    size_t length() inout
    {
        return len;
    }
    alias length _lib_obj_size__;

    /// Test is slice is valid
    @property
    bool ok() inout
    {
        return _ptr !is null;
    }

    /// Slice casting
    inout(T) opCast(T)() inout
    {
        static if(isPointer!T)
            return ptr!T;
        else
            return as!T;
    }

    /// Create a safe refrence for slicing, good for primitive type constants
    static Slice Ref(T)(T t)
    {
        align(1) static struct Ref{ T t; }
        return Slice(new Ref(t), T.sizeof);
    }
}

package:

/// Find the byte size of a valid Slice type
size_t _lib_obj_size__(P)(in P p)
    if(isSomeString!P || ((isStaticArray!P || isDynamicArray!P) && !isBanned!(ForeachType!P)))
{
    return p.length ? p[0].sizeof * p.length : 0;
}

/// Find the byte size of a valid Slice type
size_t _lib_obj_size__(P)(in P p)
    if(isBasicType!P || isPODStruct!P) 
{
    return P.sizeof;
}

/// Find the byte size of a valid Slice type
size_t _lib_obj_size__(P)(in P p)
    if(isPointer!P) 
{
    return _lib_obj_size__(*p);
}
    
/// Find the pointer of a valid Slice type
const(char)* _lib_obj_ptr__(P)(ref P p)
{
    static if((isArray!P && !isBanned!(ForeachType!P)))
        return cast(const(char*))p.ptr;
    else static if(isBasicType!P || isPODStruct!P)
        return cast(const(char*))(&p);
    else static if(isPointer!P) 
        return _lib_obj_ptr__(*p);
    else assert(false, "Not a valid type for leveldb slice: ref " ~ typeof(p).stringof);
}

template isBanned(T)
{
    static if(is(T == class) || isDynamicArray!T || isPointer!T)
        enum isBanned = true;
    else
        enum isBanned = false;
}

template isPODStruct(T)
{
    static if(is(T == struct))
        enum isPODStruct = __traits(isPOD, T);
    else
        enum isPODStruct = false;
}

unittest
{
    assert(_lib_obj_size__("1234567890") == 10);
    assert(_lib_obj_size__("1234") == 4);
    int i = 123567;
    assert(_lib_obj_size__(i) == int.sizeof);
    long l = 123567;
    assert(_lib_obj_size__(l) == long.sizeof);
    double d = 123567;
    assert(_lib_obj_size__(d) == double.sizeof);
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
    assert(s.length == int.sizeof * 2);
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

    s = Slice(new Point!real(10, 12));
    assert(s.length == real.sizeof * 2);
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
