package naviebayes;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NB_Doc_Classification {
  
  //public static HashMap<String, Double> Hmap;
		
  public static HashMap<String, Double> getHashMap(Configuration conf, String remoteFilePath){
	  
	  Path remotePath = new Path(remoteFilePath+"/part-m-00000");
	  HashMap<String, Double> Hmap = new HashMap<String, Double>();
	  
	  try (FileSystem fs = FileSystem.get(conf);
	            FSDataInputStream in = fs.open(remotePath);
	            BufferedReader d = new BufferedReader(new InputStreamReader(in));) {
	            
		  String line;	            
	      line = d.readLine();
	      System.out.println("line:"+line);
	      while ((line = d.readLine()) != null) {
	            	
	    	  String[] sstr = line.split("\t");
	    	  if(sstr.length==2){	            
	    		  double value = Double.valueOf(sstr[1].toString());	    		  
	    		  Hmap.put(sstr[0], value);
	    	  }               
	      }	      
	 } catch (IOException e) {
	    e.printStackTrace();
	 }	  
	  return Hmap;
  }
	
	
  //从第一次mapreduce的输出文件中得到posting的单词数和类别包含的总词数
  public static Pair<Integer, Integer> catNumbers(Configuration conf, String remoteFilePath) {
        Path remotePath = new Path(remoteFilePath+"/part-r-00000");
        int postNum = 0;
        int totalWordNum = 0;
        try (FileSystem fs = FileSystem.get(conf);
            FSDataInputStream in = fs.open(remotePath);
            BufferedReader d = new BufferedReader(new InputStreamReader(in));) {
            String line;
            
            line = d.readLine();
            System.out.println("line:"+line);
            while ((line = d.readLine()) != null) {
            	
            	String[] sstr = line.split("\t");
            	if(sstr.length==2){
            		
            		totalWordNum += Integer.parseInt(sstr[1]); 
            		postNum++;
            	}               
            }
      
        } catch (IOException e) {
            e.printStackTrace();
        }            

        Pair<Integer, Integer> pair = Pair.of(postNum, totalWordNum);
        //pair.getLeft();
        //pair.getRight();
        return pair;
    }
  
  //从输入文件路径中得到其所包含的文档数
  public static int get_classDocNum(Configuration conf, String remoteFilePath){
	  
	  Path remotePath = new Path(remoteFilePath);
	  try (FileSystem fs = FileSystem.get(conf);){
		  FileStatus[] fss = fs.listStatus(remotePath);
		  int count = -1;
		     if (fss.length > 0) {
		    	 count = fss.length;
		     }
		  return count;
	  }catch (IOException e) {
          e.printStackTrace();
      } 
	  return -1;
  }
	
  public static class get_post_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	  
	  private static Text keyInfo = new Text();// 存储单词和文档名组合 eg: hello 
	  private static final IntWritable valueInt = new IntWritable(1);// 存储词频,初始化为1
	  
	  @Override
	  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
		  
		  String line = value.toString();
		  //去除标点token操作
	      line = line.replaceAll("[^a-zA-Z]", " ");
	      line = line.replaceAll("\\s{2,}", " ").trim();
	      line = line.toLowerCase();
	      String[] fields = line.split(" ");// 得到字段数组
		  
	      for (String field : fields) {
	        	if(field!=null){
	        		// key值由单词和URL组成，如“MapReduce,file1”  
	        		keyInfo.set(field);  
	        		context.write(keyInfo, valueInt);  
	        	}	            
	        }  
		  
	  }
  }
  
  public static class get_post_Reducer extends Reducer<Text,IntWritable,Text,LongWritable>{
	  
	  private static Text info = new Text();  

	    @Override  //伪代码,表示重写 系统可以帮你检查正确性
	    protected void reduce(Text key, Iterable<IntWritable> values, Context context)  
	            throws IOException, InterruptedException {  
	        long sum = 0;// 统计词频  
	        for (IntWritable value : values) {  
	            sum += value.get();  
	        }  
	        
	        LongWritable SUM = new LongWritable(sum);

	        context.write(key, SUM);  
	    }  
	  
  }
	
  public static class NB_calcu_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
  	
	  private static Text keyInfo = new Text();
	  private static DoubleWritable valueInfo = new DoubleWritable(); 
	  private static int totalNum; 
	  
	    
	  @Override
      protected void setup(Context context)
              throws IOException, InterruptedException {
          try {
              //从全局配置获取配置参数
              Configuration conf = context.getConfiguration();                				
			  totalNum = conf.getInt("totalNum", 0); //这样就拿到了
			  //System.out.println(totalNum);
          } catch (Exception e) {                
              e.printStackTrace();
          }           
      }
	    
      @Override
      protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	 
    	  
    	  String[] sstr = value.toString().split("\t");
    	  if(sstr.length==2){
      		int wcount = Integer.parseInt(sstr[1]);
      		double score = Math.log(((wcount+1.0)/totalNum));
      		//System.out.print(((wcount+1)/totalNum));
      		//System.out.print("//");
      		//System.out.println(score);
      		keyInfo.set(sstr[0]);
        	valueInfo.set(score);
            context.write(keyInfo,valueInfo);
    	  }
    	  
      }
  
  }
  
  
  
  
  public static boolean run1(Configuration configuration,String inputPath,int cla){
      try {
      	
      	Job job = Job.getInstance(configuration,"NavieBayes01");
          job.setJarByClass(NB_Doc_Classification.class);
          job.setMapperClass(get_post_Mapper.class);           
          job.setReducerClass(get_post_Reducer.class);
          
          job.setMapOutputKeyClass(Text.class);
          job.setMapOutputValueClass(IntWritable.class);
          
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(LongWritable.class);

          FileInputFormat.setInputPaths(job, new Path(inputPath));
          // 指定处理完成之后的结果所保存的位置
          FileOutputFormat.setOutputPath(job, new Path("output4/class"+cla+"_out1"));
          boolean f = job.waitForCompletion(true);
          return f;
      } catch (Exception e) {
          e.printStackTrace();
      }
      return false;
  }
	
  public static boolean run2(Configuration configuration,String inputPath,int cla){
      try {
      	  Job job = Job.getInstance(configuration,"NavieBayes02");
          
      	  job.setJarByClass(NB_Doc_Classification.class);
                      
          job.setMapperClass(NB_calcu_Mapper.class);           
          job.setNumReduceTasks(0);
          job.setMapOutputKeyClass(Text.class);
          job.setMapOutputValueClass(DoubleWritable.class);

          FileInputFormat.setInputPaths(job, new Path(inputPath));
          // 指定处理完成之后的结果所保存的位置
          FileOutputFormat.setOutputPath(job, new Path("output4/class"+cla+"_out2"));
          boolean f = job.waitForCompletion(true);
          return f;
      } catch (Exception e) {
          e.printStackTrace();
      }
      return false;
  }
  
  
  public static ArrayList<File> getFiles(String path) throws Exception {
	  
	  ArrayList<File> fileList = new ArrayList<File>();
	  File file = new File(path);
	  if(file.isDirectory()){
		  File []files = file.listFiles();
		  for(File fileIndex:files){
			  //如果这个文件是目录，则进行递归搜索
			  if(fileIndex.isDirectory()){
				  getFiles(fileIndex.getPath());
				  }
			  else {
				  //如果文件是普通文件，则将文件句柄放入集合中
				  fileList.add(fileIndex);
				  }
			  }
		  }
	  return fileList;
	}
  
  public static void testAcc(Configuration conf,int fenc1,int fenc2,double Pv1 ,double Pv2) throws Exception{
	  
	     String scorePath1 = "output4/class1_out2";
	     String scorePath2 = "output4/class2_out2";
	    
	     HashMap<String, Double> hmapC1 = getHashMap(conf,scorePath1);
	     HashMap<String, Double> hmapC2 = getHashMap(conf,scorePath2);
	
	     System.out.println("hashmap size：");
	     System.out.println(hmapC1.size());	
	     System.out.println(hmapC2.size());	
	     
	     String testPath1 = "/usr/local/hadoop/work04/test/alt.atheism";
	     //String testPath2 = "/usr/local/hadoop/work04/test/alt.atheism";

	     ArrayList<File> flist1 = getFiles(testPath1);
	     int totalcount = flist1.size();
	     int scoreCount = 0;
	     for(File f:flist1){    	 
	    	 //System.out.println("文件:" + f.getAbsolutePath());	    	 
	    	// 使用ArrayList来存储每行读取到的字符串
	 		double cla1Score = 0.0;
	 		double cla2Score = 0.0;
	 		
	 		try {
	 			//File file = new File(name);
	 			InputStreamReader inputReader = new InputStreamReader(new FileInputStream(f));
	 			BufferedReader bf = new BufferedReader(inputReader);
	 			// 按行读取字符串
	 			String line;
	 			while ((line = bf.readLine()) != null) {
	 				
	 				line = line.replaceAll("[^a-zA-Z]", " ");
	 			    line = line.replaceAll("\\s{2,}", " ").trim();
	 			    line = line.toLowerCase();
	 			    String[] fields = line.split(" ");// 得到字段数组
	 			    
	 			   for (String field : fields) {
	 		        	if(field!=null){
	 		        		//CLASS 1 score
	 		        		if(hmapC1.containsKey(field)){
	 		        			//System.out.println(field);
	 		        			double dd = hmapC1.get(field);	 		        			
	 		        			cla1Score += dd;
	 		        		}	 		        				 		        			   
	 		        		else{
	 		        			cla1Score += Math.log(1.0/(fenc1));
	 		        		}	 		        				 		        		
	 		        		//CLASS 2 score
	 		        		if(hmapC2.containsKey(field)) {
	 		        			double dd = hmapC2.get(field);	 		        			
 		        				cla2Score += dd;
	 		        		}	 		        				 		        			
	 		        		else{
	 		        			cla2Score += Math.log(1.0/(fenc2));
	 		        		}
	 		        			
	 		        	}	            
	 		        }  	 			    	 				
	 			}
	 			bf.close();
	 			inputReader.close();
	 		} catch (IOException e) {
	 			e.printStackTrace();
	 		}
	 		//加上先验概率
	 		cla1Score += Math.log(Pv1);
	 		cla2Score += Math.log(Pv2);
	 		if(cla1Score>cla2Score){
	 			scoreCount++;
	 		}	    	 
	     }
	     System.out.print("class 1 correct number:");
	     System.out.println(scoreCount);
	     System.out.print("class 1 total number:");
	     System.out.println(totalcount);
	     System.out.print("class 1 accuracy:");
	     System.out.println((scoreCount+0.0)/totalcount);
	     
	     String testPath2 = "/usr/local/hadoop/work04/test/comp.graphics";

	     ArrayList<File> flist2 = getFiles(testPath2);
	     totalcount = flist2.size();
	     scoreCount = 0;
	     for(File f:flist2){    	 
	    	 //System.out.println("文件:" + f.getAbsolutePath());	    	 
	    	// 使用ArrayList来存储每行读取到的字符串
	 		double cla1Score = 0.0;
	 		double cla2Score = 0.0;
	 		
	 		try {
	 			//File file = new File(name);
	 			InputStreamReader inputReader = new InputStreamReader(new FileInputStream(f));
	 			BufferedReader bf = new BufferedReader(inputReader);
	 			// 按行读取字符串
	 			String line;
	 			while ((line = bf.readLine()) != null) {
	 				
	 				line = line.replaceAll("[^a-zA-Z]", " ");
	 			    line = line.replaceAll("\\s{2,}", " ").trim();
	 			    line = line.toLowerCase();
	 			    String[] fields = line.split(" ");// 得到字段数组
	 			    
	 			   for (String field : fields) {
	 		        	if(field!=null){
	 		        		//CLASS 1 score
	 		        		if(hmapC1.containsKey(field)){
	 		        			//System.out.println(field);
	 		        			double dd = hmapC1.get(field);	 		        			
	 		        			cla1Score += dd;
	 		        		}	 		        				 		        			   
	 		        		else{
	 		        			cla1Score += Math.log(1.0/(fenc1));
	 		        		}	 		        				 		        		
	 		        		//CLASS 2 score
	 		        		if(hmapC2.containsKey(field)) {
	 		        			double dd = hmapC2.get(field);	 		        			
 		        				cla2Score += dd;
	 		        		}	 		        				 		        			
	 		        		else{
	 		        			cla2Score += Math.log(1.0/(fenc2));
	 		        		}	 		        			
	 		        	}	            
	 		        }  	 			    	 				
	 			}
	 			bf.close();
	 			inputReader.close();
	 		} catch (IOException e) {
	 			e.printStackTrace();
	 		}
	 		//加上先验概率
	 		cla1Score += Math.log(Pv1);
	 		cla2Score += Math.log(Pv2);
	 		if(cla1Score<cla2Score){
	 			scoreCount++;
	 		}	    	 
	     }
	     System.out.print("class 2 correct number:");
	     System.out.println(scoreCount);
	     System.out.print("class 2 total number:");
	     System.out.println(totalcount);
	     System.out.print("class 2 accuracy:");
	     System.out.println((scoreCount+0.0)/totalcount);
  }
  
  public static void main(String[] args) throws Exception {
	 
	 Configuration configuration = new Configuration();
     configuration.set("fs.defaultFS", "hdfs://localhost:9000"); 
     
     String class_1_path = "train/alt.atheism"; // HDFS路径
     String class_2_path = "train/comp.graphics";
     
     
     int C1_DocNum = get_classDocNum(configuration,class_1_path);
     int C2_DocNum = get_classDocNum(configuration,class_2_path);
     //System.out.println(C2_DocNum);
     
     double Pv1 = (C1_DocNum+0.0)/(C2_DocNum+C1_DocNum);
     double Pv2 = (C2_DocNum+0.0)/(C2_DocNum+C1_DocNum);
     System.out.println("先验概率：");
     System.out.println(Pv1);
     System.out.println(Pv2);
     
     
     /*
     if (run1(configuration,class_1_path,1)&&run1(configuration,class_2_path,2)){
    	 System.out.println("stage one succeed!");  	 
     }
     */
     
     String path1 = "output4/class1_out1";
     String path2 = "output4/class2_out1";
     
     Pair<Integer, Integer> pair1 = catNumbers(configuration,path1);
     Pair<Integer, Integer> pair2 = catNumbers(configuration,path2);
     
     int fenmuC1 = pair1.getLeft()+pair1.getRight();
     int fenmuC2 = pair2.getLeft()+pair2.getRight();
     System.out.println("类别score计算分母：");
     System.out.println(fenmuC1);
     System.out.println(fenmuC2);
     /*    
     configuration.setInt("totalNum",fenmuC1);        
     if (run2(configuration,path1,1)){
    	 
    	 System.out.println("stage two succeed half!");   	 
    	 configuration.setInt("totalNum",fenmuC2);   	 
    	 if(run2(configuration,path2,2)){
    		 System.out.println("stage two succeed totally!");
    	 }   	 
     }
     */
     
     testAcc(configuration,fenmuC1,fenmuC2,Pv1 ,Pv2);
          
  }
}
