#D-Leveldb [![Build Status](https://travis-ci.org/bheads/d-leveldb.svg)](https://travis-ci.org/bheads/d-leveldb)
A Leveldb implementation for D.  Requires [leveldb deimos bindings](https://github.com/bheads/leveldb/).

Forked, to fix the use of custom comparators.

##Comarator
```
class ComparatorDbKey: Options.Comparator
{
  override int compare(const char[] a, const char[] b) inout
  {
    immutable DbKey al = *cast(const DbKey*)a;
    immutable DbKey bl = *cast(const DbKey*)b;

    auto dTime   =  cast(int)(al.gTime - bl.gTime);
    return dTime;
  }
}

```

##Example
```
import leveldb, std.stdio;

void main()
{
    auto opt = new Options;
    opt.create_if_missing = true;

    auto db = new DB(opt, "path_to_my_db_folder");
    db.put("Hello", "World");

    assert(db.get_slice("Hello").as!string == "World");

    db.put("PI", 3.14);

    foreach(Slice key, Slice value; db)
    {
        if(key.as!string == "PI")
            writeln(key.as!string, ": ", value.as!double);
        else
            writeln(key.as!string, ": ", value.as!string);
    }
}
```

##Leveldb Version: 1.16.0

### Installation
Get this with [dub](http://code.dlang.org/packages/d-leveldb).

To use this package, put the following dependency into your project's package.json into the dependencies section:
```
{
        ...
        "dependencies": {
                "d-leveldb": "~master"
        }
}
```
