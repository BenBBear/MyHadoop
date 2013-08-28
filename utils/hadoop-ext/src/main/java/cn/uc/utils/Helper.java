package cn.uc.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.StringTokenizer;

import com.google.common.collect.Lists;

public class Helper {
	/**
	 * 获取本java 的进程id
	 * @return
	 */
	public static int getPid() {  
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();  
        String name = runtime.getName(); // format: "pid@hostname"  
        try {  
            return Integer.parseInt(name.substring(0, name.indexOf('@')));  
        } catch (Exception e) {  
            return -1;  
        }  
    }  
	/**
	 * 使用StringTokenizer做字符串切分
	 * @param str
	 * @param spliter
	 * @return
	 */
	public static List<String> split(String str,String spliter){
		List<String> re = Lists.newArrayList();
		StringTokenizer st = new StringTokenizer(str,spliter);
		while(st.hasMoreTokens()){
			re.add(st.nextToken());
		}
		return re;
	}
}
