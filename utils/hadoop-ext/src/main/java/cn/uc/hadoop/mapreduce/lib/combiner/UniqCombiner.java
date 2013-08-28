package cn.uc.hadoop.mapreduce.lib.combiner;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.mapreduce.Reducer;

public class UniqCombiner<KEY, VALUE> extends Reducer<KEY, VALUE, KEY, VALUE> {
	//TODO 需要测试内存中最多保存多少个key
	public static int maxValueNumber = 10000;
	private Set<VALUE> vset;
	private KEY nowKey;
	@Override
	protected void setup(Context context)throws IOException,InterruptedException{
		vset = new TreeSet<VALUE>();
	}
	
	@Override
	protected void reduce(KEY key, Iterable<VALUE> values, Context context)
			throws IOException, InterruptedException {
		nowKey = key;
		for (VALUE value : values) {
			vset.add(value);
			if(vset.size()>=maxValueNumber){
				flush(context);
			}
		}
		flush(context);
	}
	
	private void flush(Context context) throws IOException, InterruptedException{
		for(VALUE value:vset){
			context.write(nowKey, value);
		}
	}
}
