import leveldb;
import std.stdio;
import std.string;

class ComparatorUlong: Options.Comparator
{
	override int compare(const char[] a, const char[] b) inout
	{
		ulong al = *cast(const ulong*)a;
		ulong bl = *cast(const ulong*)b;
		return cast(int)(al - bl);
	}

	override string name() inout
	{
		return "Ulong Compare";
	}
}

class LogDb{
	ComparatorUlong cmpUlong;
	Options opt;
  DB db;

	this(string name="data")
  {
    cmpUlong = new ComparatorUlong;
    opt = new Options;
		opt.comparator(cmpUlong);
		opt.compression(1);
		opt.create_if_missing = true;
    db = new DB(opt, "data");
	}

	void fill(int cnt)
  {
		for(ulong xx=0; xx < cnt; xx++){
			db.put(xx, "%d".format(xx*10));
		}
  }

  void test()
  {  		
  	foreach_reverse(Slice key, Slice value; db)
  	{
  		//writeln(key.as!string, ": ", value.as!string);
  		writeln(key.as!ulong, ": ", value.as!string);
  	}
  	writeln("----------");

  	foreach(Slice key, Slice value; db)
  	{
  		//writeln(key.as!string, ": ", value.as!string);
  		writeln(key.as!ulong, ": ", value.as!string);
  	}
  }

}

void main()
{
  LogDb db = new LogDb;
  db.fill(10);
  db.test();
	writeln("DONE");
}