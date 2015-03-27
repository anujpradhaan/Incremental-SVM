import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Accurcay {

	
	public static void main(String[] args) throws IOException
	{
		find_accurcay();
	}

	public static void find_accurcay() throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
	    
	        FileSystem filesys;
	        filesys = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"), conf);
	        
	        String content="";
	    	String line=null;
	    	BufferedReader br=null;
	    	String f1= "ensemble";
	    	String f2="testing";
	    	
	    	Path p =new Path(filesys.getWorkingDirectory()+"/ensembleresult/"+f1);
	    
	    	Path dest= new Path("/tmp");
	    	 filesys.copyToLocalFile(p, dest);
	    	 Path q=new Path(filesys.getWorkingDirectory()+"/ensembleresult/"+f2);
	    	 filesys.copyToLocalFile(p, dest);
	    	 File f=new File("/tmp/"+f1);
	    	 
	    	 ArrayList<Long> st1 =new ArrayList<Long>();
	    	 
	    	 try
	        {
	           
	            FileReader fr = new FileReader(f);
	            br = new BufferedReader(fr);
	      
	            while ((line= br.readLine()) != null)
	            {
	            	StringTokenizer tokenizer = new StringTokenizer(line," ");
	            	tokenizer.nextToken();
	            	 st1.add(Long.parseLong(tokenizer.nextToken()));
	            }
	        }
	        catch(Exception e)
	        {
	        	System.out.println("file not found"+e);
	        }
	        finally{
	        	br.close();
	        }
	    	System.out.println("--------------second file---------------");
	    	long cnt=0;
	    	int i=0;
	    try{
	    	f=new File("/tmp/"+f2);
	    	FileReader fr = new FileReader(f);
	        br = new BufferedReader(fr);
	        
	        while ((line= br.readLine()) != null)
	        {
	        	StringTokenizer tokenizer = new StringTokenizer(line," ");
	        long a1= st1.get(i);
	        	//System.out.println("token"+ tokenizer.nextToken());
	        long a2= (long) Float.parseFloat(tokenizer.nextToken());
	        System.out.println("first = "+a1+" second= "+a2);
	        i++;
	        if(a1==a2)
	        {
	        	cnt++;
	        }
	        }
	    }
	    catch(Exception e)
	    {
	    	System.out.println("file not found"+e);
	    }
	    finally{
	    	br.close();
	    }

	        
	   double acc= (cnt*100.0/i) ;
	   System.out.println("true= "+cnt);
	   System.out.println("Total" +i);
	   System.out.println("Accuracy= "+acc);
	}


}
