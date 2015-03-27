import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


	public class TestAnyCode {
			public static class TestAnyMapper extends Mapper<Object,FileInfo,Object,FileInfo>{
				public void map(Object key,FileInfo value,Context context) throws IOException, InterruptedException{
					FileSystem filesys = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),context.getConfiguration());
					String strInputPath=context.getConfiguration().get("inputPath","/");
					Path ptInputPath = new Path(strInputPath);
					System.out.println("key "+ key.toString()+" value: "+value.toString());
					FileStatus[] arFsFiles=filesys.listStatus(ptInputPath);
					FileStatus fsFile ;
					
					//context.write(key, value);
					
					//FileStatus[] status = filesys.listStatus(new Path(filesys.getWorkingDirectory()+"/svmmodels/"+key.toString()));
                    
                    //ArrayList<String> listOfFiles=new ArrayList<String>();
                    //for (int i=0;i<status.length;i++){
                    		//listOfFiles.add(status[i].getPath().toString());
                    		//status[i].getPath().getName();
                    		//System.out.println(status[i].getPath().getName());
                    		//Below Code for Reading the data out of files 
                    		BufferedReader br=new BufferedReader(new InputStreamReader(filesys.open(new Path(filesys.getWorkingDirectory()+"/tempsvmmodels/"+key.toString()))));
                    		System.out.println("***********Reading="+key);
                    		//boolean flag = Boolean.getBoolean(filesys.getConf().get("dfs.support.append"));
                    		//System.out.println("dfs.support.append is set to be " + flag);
                    		//BufferedWriter out=new BufferedWriter(new InputStreamWriter());
                            //String line;
                            /*
                            FSDataOutputStream fsout=filesys.append(new Path(filesys.getWorkingDirectory()+"/globalmodel/global.txt"));
       		             	PrintWriter pout=new PrintWriter(fsout);
       		             	//pout.write(input.read());
       		             	line=br.readLine();
                            while (line != null){
                                  //  System.out.println(line);
                                    line=br.readLine();
                                	pout.append(line);
                      //      }
                            }*/
                    		BufferedWriter fsout=new BufferedWriter(new OutputStreamWriter(filesys.create(new Path("globalmodel/globalmodel.txt"))));
            	            //fs.getWorkingDirectory()+"/globalmodel/global.txt"));
                    		 PrintWriter pout=new PrintWriter(fsout);
                            String line=null;
    		                line=br.readLine();
    		                //br.
    		                int count=1;
    		                while (line != null){
    		                        //System.out.println(line);
    		                        line=br.readLine();
    		                        if(count>8)
    		                        	if(line!=null)
    		                        		pout.append(line+"\n");
    		                        count++;
    		                //        System.out.println(line);
    		                }
					
					System.out.println("Outkey----------------");
					System.out.println("outkey "+key.toString() + "value ="+ value.toString() );
				}
			}
			
			public static class TestAnyReducer extends Reducer<Object,FileInfo,Text,Text>{
				public void reduce(Object key,Iterable<FileInfo> itrValues,Context context) throws IOException{
					
				}
			}
			
			public void appendLocalModels() throws IOException{
				FileSystem fs = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),new Configuration());
		        FileStatus[] status = fs.listStatus(new Path(fs.getWorkingDirectory()+"/tempsvmmodels"));
		        //ArrayList<FileStatus> listOfFiles=new ArrayList<FileStatus>();
	            BufferedWriter fsout=new BufferedWriter(new OutputStreamWriter(fs.create(new Path("globalmodel/globalmodel.txt"))));
	            //fs.getWorkingDirectory()+"/globalmodel/global.txt"));
	            PrintWriter pout=new PrintWriter(fsout);
	             	//pout.write(input.read());	
	            		
		        		for (int i=0;i<status.length;i++){
		        		//listOfFiles.add(status[i].getPath().toString());
		        		System.out.println(status[i].getPath());
		        		//Below Code for Reading the data out of files 
		        		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
		                String line=null;
		                line=br.readLine();
		                //br.
		                int count=1;
		                while (line != null){
		                        //System.out.println(line);
		                        line=br.readLine();
		                        if(count>8)
		                        	if(line!=null)
		                        		pout.append(line+"\n");
		                        count++;
		                //        System.out.println(line);
		                }
		        		
		        }
		        //return status;
			}

			public void testTheGlobalModel(){
				Configuration conf=new Configuration();
				try{
	                FileSystem filesystem = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),conf);
	                
	                //get the global model file and copy to the /tmp directory of master node
	                Path path=new Path(filesystem.getWorkingDirectory()+"/globalmodel/globalmodel.txt.model");
	            	filesystem.copyToLocalFile(path, new Path("/tmp"));//copy each model file to local directory
	            	
	            	//get the testing data and copy to /tmp directory of master node 
	            	/*
	            	 * Note here, there is a possibility that there are more that one testing data available.
	            	 * So, we need to fetch all the data available in files and generate a testing result for all the files. 
	            	 * */
	            	
	            	FileStatus[] status = filesystem.listStatus(new Path(filesystem.getWorkingDirectory()+"/testing"));
	                for (int i=0;i<status.length;i++){
	                	status[i].getPath();
	                	
	                	/*
	                	 * First copy all the files to /tmp directory and generate all the testing models for the same 
	                	 * */
	                	filesystem.copyToLocalFile(status[i].getPath(), new Path("/tmp"));
	                	
	                	//Build a argument array here to pass into the svm_predictor(args)
	                	Date date = new Date();
	    	            long milsec = date.getTime();
	    	            String tmpfile = new String("f_"+milsec);
	                	String args[]=new String[]{
	                			"/tmp/"+status[i].getPath().getName(),
	                			"/tmp/globalmodel.txt.model",
	                			"/tmp/"+tmpfile
	                	};
	                	
	                	svm_predictor.main(args);
	                	
	                	// now copy testing result back to hdfs in the /testingresult directory
	                	
	                	filesystem.copyFromLocalFile(new Path("/tmp/"+tmpfile), new Path(filesystem.getWorkingDirectory()+"/testingresults"));
	                	
	                }
            	}catch(Exception e){
					System.err.println(e);
				}
			}
	        public static void main (String [] args) throws Exception{
	            Job job;
	            Configuration configuration;
	        	try{
	        		TestAnyCode testany=new TestAnyCode();
	        		configuration=new Configuration();	
	        		FileSystem fs = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),configuration);
	        			testany.appendLocalModels();
	        				String as[]=new String[2];
	        				Date date = new Date();
	        	            long milsec = date.getTime();
	        	            String tmpfile = new String("/tmp/f_"+milsec);
	        	            fs.copyToLocalFile(new Path(fs.getWorkingDirectory()+"/globalmodel/globalmodel.txt"), new Path("/tmp"));
	        				as[0] = "/tmp/globalmodel.txt";
	    	                as[1] = new String(as[0]+".model");
	    	                try {
	    	                      svm_train.main(as);
	    	                } catch (IOException e) {
	    	                    throw new IOException("Training error occured.");
	    	                }
	    	                
	    	                fs.copyFromLocalFile(new Path("/tmp/globalmodel.txt.model"), new Path(fs.getWorkingDirectory()+"/globalmodel/"));
	        				//svm_train.main(args);
	        				
	    	                testany.testTheGlobalModel();
	        				
	                        
	                        //svm_train.main(new String[]{"hdfs://master1:54310/user/hduser/"});
	                        FileStatus[] status = fs.listStatus(new Path(fs.getWorkingDirectory()+"/svmmodels"));
	                        
	                        ArrayList<String> listOfFiles=new ArrayList<String>();
	                        for (int i=0;i<status.length;i++){
	                        		//listOfFiles.add(status[i].getPath().toString());
	                        		//status[i].getPath().getName();
	                        		//System.out.println(status[i].getPath().getName());
	                        		/*Below Code for Reading the data out of files 
	                        		/*BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
	                                String line;
	                                line=br.readLine();
	                                while (line != null){
	                                        //System.out.println(line);
	                                        line=br.readLine();
	                                }*/
	                        }
	                       /* 
	                        job=new Job(configuration,"buildglobal");
	                        job.setInputFormatClass(WholeFileInputFormat.class);
	                        job.setJarByClass(Main.class);
	                        job.setMapperClass(TestAnyMapper.class);
	                        job.setReducerClass(TestAnyReducer.class);
	                        job.setMapOutputValueClass(FileInfo.class);
	                        job.setMapOutputKeyClass(Text.class);
	                        job.setOutputKeyClass(Object.class);
	                        job.setOutputValueClass(Text.class);
	                        FileInputFormat.addInputPath(job, new Path(fs.getWorkingDirectory()+"/svmmodels"));
	                		FileOutputFormat.setOutputPath(job, new Path("outputtestany"));
	                		job.waitForCompletion(true);
	                	//	*/
	                        
	                }catch(Exception e){
	                        System.out.println(e);
	                }
	                
	        }
	}