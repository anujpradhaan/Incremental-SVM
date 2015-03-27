import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;


public class IncrementalSVM {

	public static class IncrementalMapper extends Mapper<Object,FileInfo,Object,FileInfo>{
		public void map(Object key,FileInfo value,Context context) throws IOException, InterruptedException{
			System.out.println("***********Reading="+key);
			/*String strBaseFilename=key.toString();
			FileSystem filesys = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),context.getConfiguration());
			FileStatus localModelFiles[]=filesys.listStatus(new Path(filesys.getWorkingDirectory()+"/svmmodels"));
			int whichFileToOpen=Integer.parseInt(strBaseFilename.substring(strBaseFilename.length()-1));
        	whichFileToOpen--;//this is decremented to make it available in the range of filestatus array
        	*/
        	context.write(key,value);
			System.out.println("Outkey----------------");
			System.out.println("outkey "+key.toString() + "value ="+ value.toString() );
			
			
			
			//new addition
			
			String[] arStrBaseSeq=null;
			String strBaseFilename,strCompFilename;
			//FileInfo fiBaseFileInfo,fiCompFileInfo;
			strBaseFilename=key.toString();
			FileSystem filesys = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),context.getConfiguration());
			//fiBaseFileInfo= new FileInfo(strBaseFilename);
			System.err.println("key is ="+strBaseFilename);
			Date date = new Date();
            long milsec = date.getTime();
            String tmpfile = new String("/tmp/f_"+milsec);
            BufferedWriter bw = null;
            String aa="";
            
            FileStatus localModelFiles[]=filesys.listStatus(new Path(filesys.getWorkingDirectory()+"/svmmodels"));
            //localModelFiles.(type[]) collection.toArray(new type[collection.size()])
            String[] filename=new String[localModelFiles.length];
            for(int temp=0;temp<localModelFiles.length;temp++){
            	filename[temp]=localModelFiles[temp].getPath().getName();
            }
            Arrays.sort(filename);
            
        	int whichFileToOpen=Integer.parseInt(strBaseFilename.substring(strBaseFilename.length()-1));
        	whichFileToOpen--;//this is decremented to make it available in the range of filestatus array
            
        	try{
            bw = new BufferedWriter(new FileWriter(tmpfile));
            //Below loop will append the incremental data file only 
            
            FileInfo a= value;
            //read the content of incremental data file
            	arStrBaseSeq=a.getLines();
            	
                for(int j=0;j<arStrBaseSeq.length;j++)
                {
                	aa=arStrBaseSeq[j]+"\n";
                	bw.write(aa.toCharArray());
                }
            
            
            
            
            //below loop will append the model file to the tmpfile file in the temp directory
        	System.err.println("selected file number is="+whichFileToOpen);
        	if(whichFileToOpen<=filename.length){
        		
        		BufferedReader br=new BufferedReader(new InputStreamReader(filesys.open(new Path(filesys.getWorkingDirectory()+"/svmmodels/"+filename[whichFileToOpen]))));
                String line=null;
                line=br.readLine();
                int count=1;
                while (line != null){
                        line=br.readLine();
                        if(count>8)
                        	if(line!=null)
                        	{   
                        		String abc[]=line.split(" ");
                    			String templine="";
                    			for(int i=0;i<abc.length;i++){
                    			//	System.out.println(abc[i]);
                    				if(i!=1 && abc[0].length()<=4)
                    					templine += abc[i]+" ";
                    			}
                    			if(abc[0].length()<=4)
                    				bw.write((line+"\n").toCharArray());
                        	}	
                        count++;
                }
        	}
            }catch(IOException e){
            	
            }finally{
            	bw.close();
            }
            
            //since we have all the data
            /*
             * i.e local data as well as incremental data
             * thus we can make a model now.
             * */
            String args[]=new String[]{
            	tmpfile,
            	tmpfile+".model"
            };
            
            System.err.println("local model file is="+(filesys.getWorkingDirectory()+"/svmmodels/"+filename[whichFileToOpen]));
            
            svm_train.main(args);
          /*
           * we need to pass the old model name as well as the new model name
           */
            replaceOldLocalModelsWithNew(tmpfile,filename[whichFileToOpen]);//source,destination
            
            //filesys.close();
            
            //testing part
            
            filesys.copyToLocalFile(new Path(filesys.getWorkingDirectory()+"/testing/testing"), new Path("/tmp"));
            
            String ss[]=null;
			//for(int j=0;j<listOfFiles.length;j++){
				//Path path=listOfFiles[j].getPath();
				//String nameoffile=path.getName();
				//filesystem.copyToLocalFile(path, new Path("/tmp"));
				//filesystem.cop
				ss=new String[]{
						"/tmp/testing",//Testing file
						tmpfile+".model",//model File
						(tmpfile)+"_model_output",
				};
				svm_predictor.main(ss);
        
				
				
				
				//copy
				
				/*
				String content="";
            	String line=null;
            	BufferedReader br=null;
            	 File f1 = new File("/tmp/model_output_"+(tmpfile));
            	try
                {
                   
                    FileReader fr = new FileReader(f1);
                    br = new BufferedReader(fr);
                    int i=0;
                    while ((line= br.readLine()) != null)
                    {
                    	i++;
                    	content+=line+"|"+i+"\n";
                    }
                }
                catch(Exception e)
                {
                	System.out.println("file not found"+e);
                }
                finally{
                	br.close();
                }

                String opfile2="/tmp/model_output_"+(tmpfile)+"_2";
                File f2=new File(opfile2);
            BufferedWriter bww=null;
            try
            {
            	FileWriter fw= new FileWriter(f2);
            	bww= new BufferedWriter(fw);
            	bww.write(content);
            }
            catch(Exception e)
            {
            	System.out.println("file not found"+e);
            }
            finally{
            	bw.close();
            }

                
                
                
                
                try
                {
                	filesys.copyFromLocalFile(new Path("/tmp/model_output_"+(tmpfile)), new Path(filesys.getWorkingDirectory()+"/testresult"));
	            } catch (IOException e) {
	                throw  new IOException("upload localfile to hdfs error.");
	            } finally {
	                
	            }
                try
                {
                	filesys.copyFromLocalFile(new Path(opfile2), new Path(filesys.getWorkingDirectory()+"/ensemblefile"));
	            } catch (IOException e) {
	                throw  new IOException("upload localfile to hdfs error.");
	            } finally {
	                
	            }
			
			*/
			
		}
	}
	
	public static class IncrementalReducer extends Reducer<Object,FileInfo,Text,Text>{
		public void reduce(Object txtKey,Iterable<FileInfo> values,Context context) throws IOException{
			//int i=0,iLcsSize;
			/*String[] arStrBaseSeq=null;
			String strBaseFilename,strCompFilename;
			//FileInfo fiBaseFileInfo,fiCompFileInfo;
			strBaseFilename=txtKey.toString();
			FileSystem filesys = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),context.getConfiguration());
			//fiBaseFileInfo= new FileInfo(strBaseFilename);
			System.err.println("key is ="+strBaseFilename);
			Date date = new Date();
            long milsec = date.getTime();
            String tmpfile = new String("/tmp/f_"+milsec);
            BufferedWriter bw = null;
            String aa="";
            
            FileStatus localModelFiles[]=filesys.listStatus(new Path(filesys.getWorkingDirectory()+"/svmmodels"));
        	int whichFileToOpen=Integer.parseInt(strBaseFilename.substring(strBaseFilename.length()-1));
        	whichFileToOpen--;//this is decremented to make it available in the range of filestatus array
            
        	try{
            bw = new BufferedWriter(new FileWriter(tmpfile));
            //Below loop will append the incremental data file only 
            for (FileInfo fiVal : values) {//read the content of incremental data file
            	arStrBaseSeq=fiVal.getLines();
            	
                for(int j=0;j<arStrBaseSeq.length;j++)
                {
                	aa=arStrBaseSeq[j]+"\n";
                	bw.write(aa.toCharArray());
                }
            }
            
            
            
            //below loop will append the model file to the tmpfile file in the temp directory
        	System.err.println("selected file number is="+whichFileToOpen);
        	if(whichFileToOpen<=localModelFiles.length){
        		
        		BufferedReader br=new BufferedReader(new InputStreamReader(filesys.open(localModelFiles[whichFileToOpen].getPath())));
                String line=null;
                line=br.readLine();
                int count=1;
                while (line != null){
                        line=br.readLine();
                        if(count>8)
                        	if(line!=null)
                        	{   
                        		String abc[]=line.split(" ");
                    			String templine="";
                    			for(int i=0;i<abc.length;i++){
                    			//	System.out.println(abc[i]);
                    				if(i!=1 && abc[0].length()<=4)
                    					templine += abc[i]+" ";
                    			}
                    			if(abc[0].length()<=4)
                    				bw.write((line+"\n").toCharArray());
                        	}	
                        count++;
                }
        	}
            }catch(IOException e){
            	
            }finally{
            	bw.close();
            }
            
            //since we have all the data
            /*
             * i.e local data as well as incremental data
             * thus we can make a model now.
             * */
            /*String args[]=new String[]{
            	tmpfile,
            	tmpfile+".model"
            };
            
            System.err.println("local model file is="+localModelFiles[whichFileToOpen].getPath().getName());
            
            svm_train.main(args);
            */
          /*
           * we need to pass the old model name as well as the new model name
           */
            //replaceOldLocalModelsWithNew(tmpfile,localModelFiles[whichFileToOpen].getPath().getName());//source,destination
            
            //filesys.close();
        }
	}
	
	public static void replaceOldLocalModelsWithNew(String src,String dest) throws IOException {
		
		FileSystem fs = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),new Configuration());
		//fs.delete(new Path(fs.getWorkingDirectory()+"/svmmodels/"+dest),true);
		fs.copyFromLocalFile(new Path(src+".model"), new Path(fs.getWorkingDirectory()+"/tempsvmmodels"));
		//fs.close();
		
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Job job;
        Configuration configuration;
    	try{
    		configuration=new Configuration();	
    		FileSystem fs = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),configuration);
		job=new Job(configuration,"buildglobal");
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setJarByClass(IncrementalSVM.class);
        job.setMapperClass(IncrementalMapper.class);
        job.setReducerClass(IncrementalReducer.class);
        job.setMapOutputValueClass(FileInfo.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Object.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(fs.getWorkingDirectory()+"/incrementaldata"));
		FileOutputFormat.setOutputPath(job, new Path("op/5"));
		job.waitForCompletion(true);
		
		//fs.close();
    	}catch(Exception e){
    		System.err.println(e);
    	}
    	
    }


	

}
