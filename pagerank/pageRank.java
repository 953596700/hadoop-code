package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparator;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;


public class pagerank {
	
	public static int totalNum;
	
    public static String catWeights(Configuration conf, String remoteFilePath) {
        Path remotePath = new Path(remoteFilePath);
        //String Swords[] = new String[100];
        //ArrayList<String> strArray = new ArrayList<String> ();
        String pweights = "";
        try (FileSystem fs = FileSystem.get(conf);
            FSDataInputStream in = fs.open(remotePath);
            BufferedReader d = new BufferedReader(new InputStreamReader(in));) {
            String line;
            int count = 0;
            while ((line = d.readLine()) != null) {
            	
            	String[] sstr = line.split("\t");
            	if(sstr.length==2){
            		String ss = sstr[0];
            		pweights+=ss+",1.0,";
            		count++;
            	}               
            }
            
            totalNum = count; 
            
        } catch (IOException e) {
            e.printStackTrace();
        }            
        return pweights;
    }
  
    public static String getLastWeights(Configuration conf, String remoteFilePath) {
        Path remotePath = new Path(remoteFilePath);
        //String Swords[] = new String[100];
        //ArrayList<String> strArray = new ArrayList<String> ();
        String pweights = "";
        try (FileSystem fs = FileSystem.get(conf);
            FSDataInputStream in = fs.open(remotePath);
            BufferedReader d = new BufferedReader(new InputStreamReader(in));) {
            String line;
            while ((line = d.readLine()) != null) {
            	
            	String[] sstr = line.split("\t");
            	if(sstr.length==2){
            		String ss = sstr[0];
            		pweights+=ss+","+sstr[1]+",";
            	}          
            }
        } catch (IOException e) {
            e.printStackTrace();
        }            
        return pweights;
    }

    public  class DescSort extends WritableComparator{
   	
    	public DescSort() {        	
        	 super(DoubleWritable.class,true);//注册排序组件
        }
         @Override
         @SuppressWarnings("all")
         public int compare(WritableComparable a, WritableComparable b) {
        	 double aa = ((DoubleWritable)a).get();
        	 double bb = ((DoubleWritable)b).get();
             if((aa-bb)>0){
            	 return -1;
             }else{
            	 return 1;
             }
         }        
    }
    
    public static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {

          return -super.compare(a, b);

        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

            return -super.compare(b1, s1, l1, b2, s2, l2);

        }
    }
    
    public static class pageOutMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
    	
    	private static DoubleWritable keyInfo = new DoubleWritable();
	    private static Text valueInfo = new Text();    	

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	//super.map(key, value, context);
            
            //若A能连接到B，则说明B是A的一条出链
        	if(!value.toString().equals("")){
        		String[] line = value.toString().split("\t");
	            String curPageName = line[0]; 
	            
	            double pr = Double.valueOf(line[1]);
	            
	            DecimalFormat df = new DecimalFormat("0.0000000000");
	            String Prvalue = df.format(pr);
	            String vout = "("+curPageName+","+Prvalue+")";
	            
	            valueInfo.set(vout);
	            keyInfo.set(pr);
	            
	            context.write(keyInfo, valueInfo);
        	}
            
        }
    }
    
    public static class pageOutReducer extends Reducer<DoubleWritable,Text,NullWritable,Text>{
    	
    	private static Text info = new Text();
    	
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	for(Text value:values){
        		info.set(value.toString());
        		context.write(NullWritable.get(),info); 
        	}                 
        }
    }
   
    public static class pageMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    	
    	private static Text keyInfo = new Text();
	    private static DoubleWritable valueInfo = new DoubleWritable();  	
	    private static String[] lastWeightsInfo;

	    @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            try {
                //从全局配置获取配置参数
                Configuration conf = context.getConfiguration();                				
				lastWeightsInfo = conf.get("lastWeights").split(","); //这样就拿到了							
            } catch (Exception e) {                
                e.printStackTrace();
            }           
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	
        	//分离邻接链表
            String[] line = value.toString().split("\t");
            if(line.length==2){
            	String curPageName = line[0];   
                String[] outPages = line[1].split(",");
                
                double outNum = outPages.length;
                double ww = 0.0;
                for(int i = 0;i<lastWeightsInfo.length;i++){
                	if(curPageName.equals(lastWeightsInfo[i])){
                		ww = Double.valueOf(lastWeightsInfo[i+1].toString())/outNum;
                		break;
                	}
                }                       
                
                for (int i=0;i<outPages.length;i++){           	
                	keyInfo.set(outPages[i]);
                	valueInfo.set(ww);
                    context.write(keyInfo,valueInfo);
                }
            }
        }
    }
   
    public static class pageReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
    	
    	private static DoubleWritable info = new DoubleWritable();
    	//private static int totalNum;
    	
    	/* @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            try {
                //从全局配置获取配置参数
                Configuration conf = context.getConfiguration();
                //totalNum = conf.getInt("totalNum", 0);
            } catch (Exception e) {                
                e.printStackTrace();
            }           
        }  */ 	
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        	double sum = 0.0;// 统计PR 
	        for (DoubleWritable value : values) {  
	            sum += value.get();  
	        }  
            sum = 0.85*sum + 0.15;
            info.set(sum);
            context.write(key, info);     
        }
    }

    public static boolean run1(Configuration configuration,int i){
        try {
        	
        	Job job = Job.getInstance(configuration,"page_rank");
            job.setJarByClass(pagerank.class);
            job.setMapperClass(pageMapper.class);           
            job.setReducerClass(pageReducer.class);
            
            //job.setMapOutputKeyClass(Text.class);
            //job.setMapOutputValueClass(DoubleWritable.class);
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.setInputPaths(job, new Path("Ex_3"));
            // 指定处理完成之后的结果所保存的位置
            FileOutputFormat.setOutputPath(job, new Path("output3/output"+i));
            boolean f = job.waitForCompletion(true);
            return f;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    
    public static boolean run2(Configuration configuration){
        try {
        	Job job = Job.getInstance(configuration,"page_rank");
            
        	job.setJarByClass(pagerank.class);
            job.setSortComparatorClass(IntWritableDecreasingComparator.class);            
            job.setMapperClass(pageOutMapper.class);           
            job.setReducerClass(pageOutReducer.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path("output3/output10"));
            // 指定处理完成之后的结果所保存的位置
            FileOutputFormat.setOutputPath(job, new Path("output3/final_output"));
            boolean f = job.waitForCompletion(true);
            return f;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    
    
    public static void main(String[] args) {
    	
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        
        //得到总的网页数，初始化网页权重为1 Ex_3/DataSet
		String remoteFilePath = "Ex_3/DataSet"; // HDFS路径   			
		String lastWeights = catWeights(configuration, remoteFilePath);		
		//System.out.println(lastWeights.split(",").length);
		//System.out.println(lastWeights);
		//System.out.println(totalNum);		 				 
		//传入全局变量
		configuration.set("lastWeights", lastWeights);
		//configuration.setInt("totalNum",totalNum);				
		
		int i=1;
		
		//先单独运行一边=遍
		run1(configuration,i);
				
		for(int s=1;s<10;s++)
		{
			remoteFilePath = "output3/output"+String.valueOf(i)+"/part-r-00000"; // HDFS路径			
			lastWeights = getLastWeights(configuration, remoteFilePath);									
			//System.out.println(lastWeights.split(",").length);
			//System.out.println(lastWeights);
			//System.out.println(lastWeights.substring(0, 100));			
			configuration.set("lastWeights", lastWeights);
												
			i++;
			run1(configuration,i);						
		}
		//remoteFilePath = "output3/output"+String.valueOf(i)+"/part-r-00000"; // HDFS路径			
		//lastWeights = getLastWeights(configuration, remoteFilePath);									
		//System.out.println(lastWeights.split(",").length);
		//System.out.println(lastWeights);
		//最后运行一次mapreduce得到符合格式的降序排序输出
		run2(configuration);
    }   
}