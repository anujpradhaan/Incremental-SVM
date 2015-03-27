import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;


public class Main {



	public static class LCSMainMapper 
	extends Mapper<Object, FileInfo, Object, FileInfo>{
		public void map(Object oKey, FileInfo fiValue, Context context
			       ) throws IOException, InterruptedException {

			FileSystem filesys = FileSystem.get(context.getConfiguration());
			String strInputPath=context.getConfiguration().get("inputPath","/");
			Path ptInputPath = new Path(strInputPath);
			System.out.println("key "+ oKey.toString()+" value: "+fiValue.toString());
			FileStatus[] arFsFiles=filesys.listStatus(ptInputPath);
			FileStatus fsFile ;
			//for(FileStatus fsFile:arFsFiles){
				
					context.write(oKey, fiValue);
					System.out.println("Outkey----------------");
					System.out.println("outkey "+oKey.toString() + "value ="+ fiValue.toString() );
					
					
					
					/////////////new addition
					
					String[] arStrBaseSeq=null;
					String strBaseFilename,strCompFilename;
					FileInfo fiBaseFileInfo,fiCompFileInfo;
					ArrayList alFiles=new ArrayList();
					strBaseFilename=oKey.toString();
					
					strBaseFilename=oKey.toString();
					fiBaseFileInfo= new FileInfo(strBaseFilename);
					
					 Date date = new Date();
			            long milsec = date.getTime();
			            String tmpfile = new String("/tmp/f_"+milsec);
			            BufferedWriter bw = null;
			            String aa="";
			            FileInfo a = fiValue;
			            
			            	arStrBaseSeq=a.getLines();
			            	
			                for(int j=0;j<arStrBaseSeq.length;j++)
			                {
			                	aa+=arStrBaseSeq[j]+"\n";
			                }
			                //bw = new BufferedWriter(new FileWriter(tmpfile));
			                //bw.write(aa.toCharArray());
			            
			            if(aa.length()!=0){
				            try {
				            	
				                bw = new BufferedWriter(new FileWriter(tmpfile));
				                //arStrBaseSeq= fiBaseFileInfo.getLines();
				                //String aa="";
				                //for(int j=0;j<arStrBaseSeq.length;j++)
				                //{
				                	//aa+=arStrBaseSeq[j];
				                //}
				                //fiBaseFileInfo.setContent(aa);
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
				                
				                //OutputStream tempout=filesystem.create(new Path(filesystem.getWorkingDirectory()+"/svmmodels/global.txt"));
				                //FSDataInputStream input=filesystem.open(new Path((filesystem.getWorkingDirectory()+"/svmmodels/"+tmpfile.substring(5)+".model")));
				      //tempout.write(1);
				      //tempout.close();
				                //new LineReader(input);
				                //System.err.println(tmpfile.substring(5));
				        //        filesystem.concat(new Path(filesystem.getWorkingDirectory()+"/svmmodels/global.txt"),new Path[]{new Path((filesystem.getWorkingDirectory()+"/svmmodels/"+tmpfile.substring(5)+".model"))});
				                //SequenceFile.
				             //FSDataOutputStream fsout=filesystem.append(new Path(filesystem.getWorkingDirectory()+"/svmmodels/global.txt"));
				             //PrintWriter pout=new PrintWriter(fsout);
				             //pout.write(input.read());
				             
				             
				                //file
				            } catch (IOException e) {
				            	System.err.println(e);
				                throw  new IOException("upload localfile to hdfs error.");
				            } finally {
				                IOUtils.closeStream(out);
				                in.close();
				            }
			            }
					
				
			//}

		}
	}

	public static class LCSMainReducer 
	extends Reducer<Object,FileInfo,Text,Text> {
		public void reduce(Object txtKey, Iterable<FileInfo> itrFiValues, 
				Context context
				) throws IOException, InterruptedException {
			int i=0,iLcsSize;
			
		}
	}
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Job jbAllLcsJob;
		Configuration cLCSJobConf;
		String[] arStrOtherArgs;
		String strTempOutputPath,strOutputDir,strInputDir,strK;
		Path ptKDocsPath;
		FileSystem filesys;
		FileStatus[] arFsFiles;
			int iTempFiles;

		cLCSJobConf = new Configuration();
		arStrOtherArgs = new GenericOptionsParser(cLCSJobConf, args).getRemainingArgs();		
		if (arStrOtherArgs.length != 2) {
			System.err.println("Usage: LCSMain <inputdir> <outputdir> <K>");
			System.exit(2);
		}
		strOutputDir=arStrOtherArgs[1];
		strInputDir=arStrOtherArgs[0];
		
		//filesys = FileSystem.get(cLCSJobConf);
		filesys = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"), cLCSJobConf);
		//filesys.
		//Creating first job
		cLCSJobConf.set("inputPath",arStrOtherArgs[0]);
		jbAllLcsJob = new Job( cLCSJobConf,"wordcount");
		jbAllLcsJob.setInputFormatClass(WholeFileInputFormat.class);
		jbAllLcsJob.setJarByClass(Main.class);
		jbAllLcsJob.setMapperClass(LCSMainMapper.class);
		jbAllLcsJob.setReducerClass(LCSMainReducer.class);
		jbAllLcsJob.setMapOutputValueClass(FileInfo.class);
		jbAllLcsJob.setMapOutputKeyClass(Text.class);
		jbAllLcsJob.setOutputKeyClass(Object.class);
		jbAllLcsJob.setOutputValueClass(Text.class);
		System.out.println(filesys.getWorkingDirectory());
//		System.exit(0);
		FileInputFormat.addInputPath(jbAllLcsJob, new Path(filesys.getWorkingDirectory()+"/inputsvmfiles"));
		if(!filesys.isDirectory(new Path("output"))){
			System.out.println("sad");
			filesys.delete(new Path("output"));
		}
		FileOutputFormat.setOutputPath(jbAllLcsJob, new Path("op/11"));
		jbAllLcsJob.waitForCompletion(true);
		
		//After this job is finished we have local models available in the hdfs directory.
		
		/*Now We will generate a global model in a separate directory*/
		
		
		
		//Testing job
		
		jbAllLcsJob = new Job(cLCSJobConf,"testsvm");
		jbAllLcsJob.setInputFormatClass(WholeFileInputFormat.class);
		jbAllLcsJob.setJarByClass(TestingMain.class);
		jbAllLcsJob.setMapperClass(TestingMain.TestingMainMapper.class);
		jbAllLcsJob.setReducerClass(TestingMain.TestingMainReducer.class);
		jbAllLcsJob.setMapOutputValueClass(FileInfo.class);
		jbAllLcsJob.setMapOutputKeyClass(Text.class);
		jbAllLcsJob.setOutputKeyClass(Object.class);
		jbAllLcsJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jbAllLcsJob, new Path(filesys.getWorkingDirectory()+"/svmmodels"));
		FileOutputFormat.setOutputPath(jbAllLcsJob, new Path("op/22"));
		jbAllLcsJob.waitForCompletion(true);
		
		
		
	}



}