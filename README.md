#D-Leveldb
A Leveldb implementation for D.  Requires [leveldb deimos bindings](https://github.com/bheads/leveldb/).

##Example
```
import etc.leveldb.leveldb;

void main()
{
    auto opt = new Options;
    opt.create_if_missing = true;

    auto db = new DB("path/to/db", opt);
    db.put("Hello", "World");

    assert(db.find("Hello", "") == "World");
    assert(db.find("Hello2", "") == "");

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
Get this with [dub](http://registry.vibed.org/packages/d-leveldb).

To use this package, put the following dependency into your project's package.json into the dependencies section:
```
{
        ...
        "dependencies": {
                "d-leveldb": "~develop"
        }
}
```
