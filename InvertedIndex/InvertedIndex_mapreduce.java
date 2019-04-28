package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.ArrayList;


public class InvertedIndex_mapreduce {
	//public static ArrayList<String> sword;
	//public static String sword;
	//倒排索引mapper类
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{

	    private static Text keyInfo = new Text();// 存储单词和文档名组合 eg: hello 
	    private static final Text valueInfo = new Text("1");// 存储词频,初始化为1  

	    @Override  
	    protected void map(LongWritable key, Text value, Context context)  
	            throws IOException, InterruptedException {  
	    		    	
	        String line = value.toString(); 
	        
	        //去除标点token操作
	        line = line.replaceAll("[^a-zA-Z0-9]", " ");
	        line = line.replaceAll("\\s{2,}", " ").trim();
	        line = line.toLowerCase();
	        String[] fields = line.split(" ");// 得到字段数组  

	        FileSplit fileSplit = (FileSplit) context.getInputSplit();// 得到这行数据所在的文件切片  
	        String fileName = fileSplit.getPath().getName();// 根据文件切片得到文件名  

	        for (String field : fields) {
	        	if(field!=null){
	        		// key值由单词和URL组成，如“MapReduce:file1”  
	        		keyInfo.set(field + "," + fileName);  
	        		context.write(keyInfo, valueInfo);  
	        	}	            
	        }  
	    }  

	}

	//倒排索引combiner类
	public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>{

	    private static Text info = new Text();  

	    // 输入： <MapReduce:file3 {1,1,...}>  
	    // 输出：<MapReduce file3:2>  
	    @Override  //伪代码,表示重写 系统可以帮你检查正确性
	    protected void reduce(Text key, Iterable<Text> values, Context context)  
	            throws IOException, InterruptedException {  
	        int sum = 0;// 统计词频  
	        for (Text value : values) {  
	            sum += Integer.parseInt(value.toString());  
	        }  
	        
	        int splitIndex = key.toString().indexOf(",");  
	        // 重新设置 value 值由 URL 和词频组成  
	        info.set(key.toString().substring(splitIndex + 1) + "," + sum);  
	        // 重新设置 key 值为单词  
	        key.set(key.toString().substring(0, splitIndex));  

	        context.write(key, info);  
	    }  
	}
	
	public static String catStopWords(Configuration conf, String remoteFilePath) {
        Path remotePath = new Path(remoteFilePath);
        //String Swords[] = new String[100];
        //ArrayList<String> strArray = new ArrayList<String> ();
        String sword = "";
        try (FileSystem fs = FileSystem.get(conf);
            FSDataInputStream in = fs.open(remotePath);
            BufferedReader d = new BufferedReader(new InputStreamReader(in));) {
            String line;
            while ((line = d.readLine()) != null) {
            	
            	line = line.replaceAll("[^a-zA-Z0-9]", "");
            	if(line!=null)
            		sword+=line+",";
            		//strArray.add(line);                
            }
        } catch (IOException e) {
            e.printStackTrace();
        }            
        return sword;
    }
	
	
	//倒排索引reducer类
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{
	    private static Text result = new Text();  
    	private static String[] fields;
	    @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            try {
                //从全局配置获取配置参数
                Configuration conf = context.getConfiguration();
                String Str = conf.get("swords"); //这样就拿到了
                fields = Str.split(",");// 得到字段数组
                            
            } catch (Exception e) {                
                e.printStackTrace();
            }
            
        }
	    // 输入：<MapReduce file3,2>  
	    // 输出：<MapReduce file1,1;file2,1;file3,2;>  
	    
	    @Override  
	    protected void reduce(Text key, Iterable<Text> values, Context context)  
	            throws IOException, InterruptedException {  
	    	
	        // 生成文档列表  
	        String fileList = new String();  
	        int totalNum = 0;
	        for (Text value : values) { 
	        	String va = value.toString();
	        	int index = va.indexOf(',');
	        	String subva = va.substring(index+1);
	        	int num = Integer.valueOf(subva);
	        	totalNum += num;
	            fileList += "<" + va + ">;";  
	        }  
	        fileList += "<total," + String.valueOf(totalNum)+">.";
	        result.set(fileList);  
	        //去除停用词
	        String k = key.toString();
	        k = k.replaceAll("[^a-z0-9]", "");	        	        
	        if(k!=null){
	        	boolean tag = true;
	 	        for(String tmp:fields){
	 	            //System.out.println(tmp);
	 	            if(tmp.equals(k)){  
	 	            	tag = false;
	 	            	break;
	 	        	}
	 	        }	 	        
	 	        if(tag){
	 	        	context.write(key, result);
	 	        }
	        }	          
	    }  
	}

	//主程序入口
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //进行配置
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS", "hdfs://localhost:9000");
        
        //打开停用词，对读取的单词进行筛选去除
    	
		String remoteFilePath = "stop_words/stop_words_eng.txt"; // HDFS路径   	
		//String remoteFilePath = "stop_words/stop_words_eng.txt"; // HDFS路径
		String sword = catStopWords(conf, remoteFilePath);
		//System.out.println(sword);
		conf.set("swords", sword);
    	//for(String tmp:sword){
    	//	System.out.println(tmp);
    	//}
    	//System.out.println(sword);
    	Job job = Job.getInstance(conf,"inverted_index");
        job.setJarByClass(InvertedIndex_mapreduce.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("txt_input"));
        // 指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(job, new Path("output"));

        // 向集群提交这个 job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
