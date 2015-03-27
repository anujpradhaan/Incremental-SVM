

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;

public  class FileInfo implements   Writable{
	private Text txtFilename;
	private Text txtContent;
	public FileInfo(){	super();
		
	}
	public FileInfo(String strFilename){
super();
		this.txtFilename=new Text(strFilename);
		this.txtContent=null;
	}
	public void setContent(String strContent){
		this.txtContent=new Text(strContent);
	}
	public FileInfo(String strFilename,String strContent){
super();
		this.txtFilename=new Text(strFilename);
		this.txtContent=new Text(strContent);
	}
	public Text getFilename(){
		return txtFilename;
	}
	public Text getContent(){
		return txtContent;
	}
	public String[] getLines(){
		return txtContent.toString().split("\n");
	}

	public FileInfo clone(){
		return new FileInfo(txtFilename.toString(),txtContent.toString());
	}
public void readFields(DataInput diIn) throws IOException {
	txtFilename=new Text();
	txtContent=new Text();
    	txtFilename.readFields(diIn);
    	txtContent.readFields(diIn);
  }

public void write(DataOutput doOut) throws IOException {
  
	this.txtFilename.write(doOut);
	this.txtContent.write(doOut);
  }




}
