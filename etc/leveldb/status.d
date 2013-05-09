module etc.leveldb.status;

private import std.conv : to;

struct Status
{
private:
    bool _ok = true;
    string msg = "success";

public:
    this(char* errptr)
    {
        if(errptr)
        {
            _ok = false;
            msg = to!string(errptr);
        }
        else
        {
            _ok = true;
            msg = "success";
        }
    }

    this(string error)
    {
        _ok = false;
        msg = error;
    }

    ref Status opAssign(char* errptr)
    {
        if(errptr)
        {
            _ok = false;
            msg = to!string(errptr);
        }
        else
        {
            _ok = true;
            msg = "success";
        }
        return this;
    }

    @property bool ok()
    {
        return _ok;
    }

    @property string toString()
    {
        return msg;
    }
}
