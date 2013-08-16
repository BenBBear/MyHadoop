package cn.uc.hadoop.mapreduce.tools.dictreplace;

import java.io.IOException;

public interface DictReplaceTask {
	public boolean waitForCompletion(boolean verbose ) throws IOException, InterruptedException, ClassNotFoundException ;
}
