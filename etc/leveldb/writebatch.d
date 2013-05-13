/**
 * D-LevelDB Write Batch
 *
 * WriteBatch holds multiple writes that can be applied to an open DB in a sinlge
 * atomic update.
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
module etc.leveldb.writebatch;

private import etc.leveldb.exceptions,
    etc.leveldb.slice;
private import deimos.leveldb.leveldb;

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

public:
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

    void put(Slice key, Slice val)
    {
        leveldb_writebatch_put(_ptr, key.ptr!(const(char*)), key.length, 
                val.ptr!(const(char*)), val.length);
    }

    void del(Slice key)
    {
        leveldb_writebatch_delete(_ptr, key.ptr!(const(char*)), key.length);
    }

    void iterate(Visitor visitor)
    {
        leveldb_writebatch_iterate(_ptr, cast(void*)&visitor,
            &batchPut, &batchDel);
    }

    void iterate(void delegate(Slice key, Slice value) puts,
        void delegate(Slice key) dels)
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
        void delegate(Slice key, Slice value) puts;
        void delegate(Slice key) dels;
    }
} // WriteBatch


private:
extern(C):

void batchPut(void* state, const char* k, size_t klen, const char* v, size_t vlen)
{
    auto visitor = cast(WriteBatch.Visitor*)state;
    visitor.puts(Slice(cast(void*)k, klen, true), Slice(cast(void*)v, vlen, true));
}

void batchDel(void* state, const char* k, size_t klen)
{
    auto visitor = cast(WriteBatch.Visitor*)state;
    visitor.dels(Slice(cast(void*)k, klen, true));
}
