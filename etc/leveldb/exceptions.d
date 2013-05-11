/**
    LevelDB exception type.

    Copyright: © 2013 Byron Heads
    License: Distributed under the Boost Software License, Version 1.0.
            (See accompanying file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)
    Authors: Byron Heads
*/

module etc.leveldb.exceptions;

private import std.conv : to;

/// Default LevelDB Exception
class LeveldbException : Exception
{
    this(char* errptr, string file = __FILE__, size_t line = __LINE__, Throwable next = null)
    {
        super(to!string(errptr), file, line, next);
    }

    this(string errstr, string file = __FILE__, size_t line = __LINE__, Throwable next = null)
    {
        super(errstr, file, line, next);
    }
}
