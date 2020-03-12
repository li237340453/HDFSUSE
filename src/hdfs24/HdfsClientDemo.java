package hdfs24;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;


public class HdfsClientDemo {
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		conf.set("dfs.republication", "2");
		conf.set("dfs.blocksize","64m");
		FileSystem fs=FileSystem.get(new URI("hdfs://192.168.80.11:9000/"), conf, "root");
		fs.copyFromLocalFile(new Path("D:\\PDF\\Foxit PhantomPDF\\SendCrashReport.exe"), new Path("/play"));
		fs.close();
	}
	
	public FileSystem fs;
	
	@Before
	public void init() throws Exception{
		Configuration conf=new Configuration();
		conf.set("dfs.republication", "2");
		conf.set("dfs.blocksize","64m");
		fs=FileSystem.get(new URI("hdfs://192.168.80.11:9000/"), conf, "root");
	}
	
	@Test
	public void testdown() throws Exception{
		fs.copyToLocalFile(new Path("/jdk-8u181-linux-x64.tar.gz"), new Path("g:\\"));
		fs.close();
	}
	@Test
	public void testmv() throws Exception{
		fs.rename(new Path("/jdk-8u181-linux-x64.tar.gz"), new Path("/play/jdk.tar.gz"));
		fs.close();
	}
	@Test
	public void testmkdir() throws Exception{
		fs.mkdirs(new Path("/play/play/play"));
		fs.close();
	}
	@Test
	public void testdelete() throws Exception{
		fs.delete(new Path("/play/play"), true);
		fs.close();
	}
	@Test
	public void testls() throws Exception{
		RemoteIterator<LocatedFileStatus>ri=fs.listFiles(new Path("/"), true);
		while (ri.hasNext()) {
			LocatedFileStatus lf=ri.next();
			
		}
		fs.close();
	}
	@Test
	public void testReadData() throws Exception {
		FSDataInputStream in=fs.open(new Path("/text"));
//		byte[] buf=new byte[1024];
//		in.read(buf);
//		System.out.println(new String(buf));
		BufferedReader br=new BufferedReader(new InputStreamReader(in,"UTF-8"));
		String line=null;
		while ((line=br.readLine() )!= null) {
			System.out.println(line);
		}
		br.close();
		in.close();
		fs.close();
	}
	@Test
	public void testRandomReadData() throws Exception{
		FSDataInputStream in=fs.open(new Path("/xx.txt"));
		byte[] buf=new byte[2];
		in.seek(17);
		in.read(buf);
		System.out.println(new String(buf));
		in.close();
		fs.close();
	}
	@Test
	public void testWriteData() throws Exception{
		FSDataOutputStream out=fs.create(new Path("/zz.jpg"), true);
		FileInputStream in=new FileInputStream("G:\\ÌÚÑ¶»º´æ\\20190617185127.jpg");
		byte[] buf=new byte[1024];
		int read=0;
		while((read=in.read(buf))!=-1) {
			out.write(buf, 0, read);
		}
		in.close();
		out.close();
		fs.close();
	}
}
