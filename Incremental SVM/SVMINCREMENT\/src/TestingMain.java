import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class TestingMain {
	public static class TestingMainMapper extends Mapper<Object, FileInfo, Object, FileInfo>{
		public void map(Object oKey, FileInfo fiValue, Context context
			       ) throws IOException, InterruptedException {

			FileSystem filesys = FileSystem.get(context.getConfiguration());
			String strInputPath=context.getConfiguration().get("inputPath","/");
			Path ptInputPath = new Path(strInputPath);
			System.out.println("key "+ oKey.toString()+" value: "+fiValue.toString());
			FileStatus[] arFsFiles=filesys.listStatus(ptInputPath);
			FileStatus fsFile ;
			context.write(oKey, fiValue);
			System.out.println("Outkey----------------");
			System.out.println("outkey "+oKey.toString() + "value ="+ fiValue.toString() );
			
			
			
			
			//new Addition
			
			try{
			Configuration conf=new Configuration();
            FileSystem filesystem = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),conf);
            
            
            Path path=new Path(filesystem.getWorkingDirectory()+"/svmmodels/"+(oKey.toString()));
        	filesystem.copyToLocalFile(path, new Path("/tmp"));//copy each model file to local directory
            
        	//now copy the test data
        	FileStatus []listOfFiles=filesystem.listStatus(new Path(filesystem.getWorkingDirectory()+"/testing"));;
        	//path=new Path(filesystem.getWorkingDirectory()+"/testsvmfiles/"+txtKey);
        	Path testfilepath=listOfFiles[0].getPath();
        	System.err.println(testfilepath.getName());
        	filesystem.copyToLocalFile(testfilepath, new Path("/tmp"));
        	
            //FileStatus []listOfFiles=getModelList();
            //t
			//Iterator<String> itr=listOfFiles.iterator();
			String ss[]=null;
			//for(int j=0;j<listOfFiles.length;j++){
				//Path path=listOfFiles[j].getPath();
				//String nameoffile=path.getName();
				//filesystem.copyToLocalFile(path, new Path("/tmp"));
				//filesystem.cop
				ss=new String[]{
						"/tmp/"+testfilepath.getName(),//Testing file
						"/tmp/"+oKey.toString(),//model File
						"/tmp/model_output"+(oKey.toString()),
				};
				svm_predictor.main(ss);
			//}
				//System.err.println(ss.length);
				
			String opfile="/tmp/model_output"+(oKey.toString());	
				
				
				String content="";
            	String line=null;
            	BufferedReader br=null;
            	 File f1 = new File(opfile);
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

                String opfile2=opfile+"_2";
                File f2=new File(opfile2);
            BufferedWriter bw=null;
            try
            {
            	FileWriter fw= new FileWriter(f2);
            	bw= new BufferedWriter(fw);
            	bw.write(content);
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
                	filesystem.copyFromLocalFile(new Path(opfile), new Path(filesystem.getWorkingDirectory()+"/testingresults"));
	            } catch (IOException e) {
	                throw  new IOException("upload localfile to hdfs error.");
	            } finally {
	                
	            }
                try
                {
                	filesystem.copyFromLocalFile(new Path(opfile2), new Path(filesystem.getWorkingDirectory()+"/ensemblefile"));
	            } catch (IOException e) {
	                throw  new IOException("upload localfile to hdfs error.");
	            } finally {
	                
	            }
			
			
				
		//}
    }
	catch(IOException e){
		System.err.println(e);
	}
	
			
			
		}
	}

	public static class TestingMainReducer 
	extends Reducer<Object,FileInfo,Text,Text> {
		public void reduce(Object txtKey, Iterable<FileInfo> itrFiValues,Context context) throws IOException, InterruptedException {
			int i=0,iLcsSize;
			String[] arStrBaseSeq=null;
			String strBaseFilename,strCompFilename;
			FileInfo fiBaseFileInfo,fiCompFileInfo;
			ArrayList alFiles=new ArrayList();
			strBaseFilename=txtKey.toString();
			
			strBaseFilename=txtKey.toString();
			fiBaseFileInfo= new FileInfo(strBaseFilename);
			/*
			 	Date date = new Date();
	            long milsec = date.getTime();
	            String tmpfile = new String("/tmp/f_"+milsec);
	            BufferedWriter bw = null;
	            String aa="";
	            for (FileInfo fiVal : itrFiValues) {
	            	arStrBaseSeq=fiVal.getLines();
	            	
	                for(int j=0;j<arStrBaseSeq.length;j++)
	                {
	                	aa+=arStrBaseSeq[j]+"\n";
	                }
	            }
	            if(aa.length()!=0){
		            try {
		            	
		                bw = new BufferedWriter(new FileWriter(tmpfile));
		                bw.write(aa.toCharArray());
		            } catch (IOException e) {
		                throw new IOException("Write local file error.");
		            } finally {
		                bw.close();
		            }
		            
		            String[] as = new String[2];
	                as[0] = tmpfile;
	                as[1] = new String(as[0]+".model");
	                try {
	                      svm_train.main(as);
	                } catch (IOException e) {
	                    throw new IOException("Training error occured.");
	                }
	                BufferedInputStream in=null;
		            OutputStream out=null;
		            try{
			            in = new BufferedInputStream(new FileInputStream((tmpfile)));
		                Configuration conf=new Configuration();
		                FileSystem filesystem = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),conf);
		                out=filesystem.create(new Path(tmpfile));
		                IOUtils.copyBytes(in, out, conf);
		                filesystem.copyFromLocalFile(new Path(tmpfile+".model"), new Path(filesystem.getWorkingDirectory()+"/svmmodels"));
		            } catch (IOException e) {
		            	System.err.println(e);
		                throw  new IOException("upload localfile to hdfs error.");
		            } finally {
		                IOUtils.closeStream(out);
		                in.close();
		            }
	            }*/
				/*try{
					//copyTestDataToTmpDirectory();//this will copy the test data to each machines local directory so that
					//they can perform there local testing
					/*Date date = new Date();
		            long milsec = date.getTime();
		            String tmpfile = new String("/tmp/ftestdata_"+milsec);
		            BufferedWriter bw = null;
		            String aa="";
		            for (FileInfo fiVal : itrFiValues) {
		            	arStrBaseSeq=fiVal.getLines();
		            	
		                for(int j=0;j<arStrBaseSeq.length;j++)
		                {
		                	aa+=arStrBaseSeq[j]+"\n";
		                }
		            }
		            if(aa.length()!=0){
			            try {
			            	bw = new BufferedWriter(new FileWriter(tmpfile));
			                bw.write(aa.toCharArray());
			            } catch (IOException e) {
			                throw new IOException("Write local file error.");
			            } finally {
			                bw.close();
			            }
			          */ 
			            //Till here we have copied the testing data to the local directory.
			            
			            //now do testing
			     /*       Configuration conf=new Configuration();
		                FileSystem filesystem = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),conf);
		                
		                
		                Path path=new Path(filesystem.getWorkingDirectory()+"/svmmodels/"+(txtKey.toString()));
		            	filesystem.copyToLocalFile(path, new Path("/tmp"));//copy each model file to local directory
		                
		            	//now copy the test data
		            	FileStatus []listOfFiles=filesystem.listStatus(new Path(filesystem.getWorkingDirectory()+"/testing"));;
		            	//path=new Path(filesystem.getWorkingDirectory()+"/testsvmfiles/"+txtKey);
		            	Path testfilepath=listOfFiles[0].getPath();
		            	System.err.println(testfilepath.getName());
		            	filesystem.copyToLocalFile(testfilepath, new Path("/tmp"));
		            	
		                //FileStatus []listOfFiles=getModelList();
		                //t
						//Iterator<String> itr=listOfFiles.iterator();
						String ss[]=null;
						//for(int j=0;j<listOfFiles.length;j++){
							//Path path=listOfFiles[j].getPath();
							//String nameoffile=path.getName();
							//filesystem.copyToLocalFile(path, new Path("/tmp"));
							//filesystem.cop
							ss=new String[]{
									"/tmp/"+testfilepath.getName(),//Testing file
									"/tmp/"+txtKey.toString(),//model File
									"/tmp/model_output"+(txtKey.toString()),
							};
							svm_predictor.main(ss);
						//}
							//System.err.println(ss.length);
							
					//}
			    }
				catch(IOException e){
					System.err.println(e);
				}
				*/
				
		}
	}//*/}}
	public static FileStatus[] getModelList() throws IOException{
		FileSystem fs = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"),new Configuration());
        FileStatus[] status = fs.listStatus(new Path(fs.getWorkingDirectory()+"/svmmodels"));
        //ArrayList<FileStatus> listOfFiles=new ArrayList<FileStatus>();
        //f/or (int i=0;i<status.length;i++){
        	//	listOfFiles.add(status[i].getPath().toString());
        		//System.out.println(status[i].getPath());
        		/*Below Code for Reading the data out of files 
        		/*BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                line=br.readLine();
                while (line != null){
                        //System.out.println(line);
                        line=br.readLine();
                }*/
        		
        //}
        return status;
	}
}
