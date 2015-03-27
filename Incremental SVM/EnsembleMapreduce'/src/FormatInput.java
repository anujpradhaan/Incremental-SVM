import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FormatInput {

	public static void main(String[] args) throws IOException {
		FileSystem filesys = FileSystem.get(new Configuration());
		BufferedReader br=new BufferedReader(new InputStreamReader(filesys.open(new Path("1.txt"))));
		BufferedWriter fsout=new BufferedWriter(new OutputStreamWriter(filesys.create(new Path("2.txt"))));
        PrintWriter pout=new PrintWriter(fsout);
		String line=br.readLine();
		do{
			String abc[]=line.split(" ");
			String templine="";
			for(int i=0;i<abc.length;i++){
				System.out.println(abc[i]);
				if(i!=1 && abc[0].length()<=4)
					templine += abc[i]+" ";
			}
			if(abc[0].length()<=4)
				pout.write(templine+"\n");
			line=br.readLine();
		}while(line!=null);
		fsout.close();
		br.close();
    }
}
