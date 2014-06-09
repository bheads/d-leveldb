/**
 * D-LevelDB Exceptions
 *
 * The library code should only throw these kind of exceptions.
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
module leveldb.exceptions;

private import std.conv : to;
private import std.exception : enforceEx;

/**
 * Base Exception type for library.  We don't do anything fancy, just have a common
 * exception type that can be caught when using this library.
 */
class LeveldbException : Exception
{
    /// LevelDB returns errors as an internal char*
    this(const(char*) errptr, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(to!string(errptr), file, line, next);
    }

    /// Take regular D strings for errors
    this(string errstr, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(errstr, file, line, next);
    }

    /// Take regular D strings for errors
    this(string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super("Unknown error",file, line, next);
    }
}

alias dbEnforce = enforceEx!LeveldbException;
