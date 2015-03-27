

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.*;

import java.io.*;


/**
 * Splitter that reads a whole file as a single record
 * This is useful when you have a large number of files
 * each of which is a complete unit - for example XML Documents
 */
public class WholeFileInputFormat extends FileInputFormat<Text, FileInfo> {

	@Override
		public  RecordReader<Text, FileInfo> createRecordReader(InputSplit inpsSplit,
				TaskAttemptContext context) {
			return new MyWholeFileReader();
		}

	@Override
		protected boolean isSplitable(JobContext context, Path ptFile) {
			return false;
		}

	/**
	 * Custom RecordReader which returns the entire file as a
	 * single value with the name as a key
	 * Value is the entire file
	 * Key is the file name
	 */
	public static class MyWholeFileReader extends RecordReader<Text, FileInfo> {

		private CompressionCodecFactory compressionCodecs = null;
		private long lStart;
		private long lEnd;
		private LineReader lrIn;
		private Text txtKey = null;
		private FileInfo fiValue = null;
		private Text txtBuffer = new Text();	
		private String strFilename;


		public void initialize(InputSplit inpsGenericSplit,
				TaskAttemptContext context) throws IOException {
			FileSplit fsSplit;
			Configuration cJob;

			fsSplit = (FileSplit) inpsGenericSplit;
			final Path ptFile = fsSplit.getPath();
			cJob = context.getConfiguration();
			compressionCodecs = new CompressionCodecFactory(cJob);
			final CompressionCodec codec = compressionCodecs.getCodec(ptFile);
			lEnd = lStart + fsSplit.getLength();
			lStart = fsSplit.getStart();
			
			
			// open the file and seek to the start of the split
			FileSystem filesys = ptFile.getFileSystem(cJob);
			FSDataInputStream fileIn = filesys.open(fsSplit.getPath());
			strFilename=fsSplit.getPath().getName();
			if (codec != null) {
				lrIn = new LineReader(codec.createInputStream(fileIn),cJob);
				lEnd = 1024;
			}
			else {
				lrIn = new LineReader(fileIn, cJob);
			}
			if (txtKey == null) {
				txtKey = new Text();
			}
			if (fiValue == null) {
				fiValue = new FileInfo(strFilename);//,"T");
			}

		}

		public boolean nextKeyValue() throws IOException {
			int iNewSize = 0;
			StringBuilder sb = new StringBuilder();
			String str,sContent;
			iNewSize = lrIn.readLine(txtBuffer);
			while (iNewSize > 0) {
				str= txtBuffer.toString();
				sb.append(str);
				sb.append("\n");
				iNewSize = lrIn.readLine(txtBuffer);
			}

			sContent = sb.toString();

			fiValue.setContent(sContent);
			txtKey=fiValue.getFilename();

			if (sb.length() == 0) {
				txtKey = null;
				fiValue = null;
				return false;
			}
			else {
				return true;
			}
		}

		@Override
			public Text getCurrentKey() {
				return txtKey;
			}

		@Override
			public FileInfo getCurrentValue() {		
				return fiValue;
			}

		/**
		 * Get the progress within the split
		 */
		public float getProgress() {
			return 0.0f;
		}

		public synchronized void close() throws IOException {
			if (lrIn != null) {
				lrIn.close();
			}
		}
	}

}


