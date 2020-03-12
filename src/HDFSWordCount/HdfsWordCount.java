package HDFSWordCount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsWordCount {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		Properties props = new Properties();
		props.load(HdfsWordCount.class.getResourceAsStream("/HdfsWordCount/job.properties"));
		Class<?> mapper_class = Class.forName(props.getProperty("MAPPER_CLASS"));
		Mapper mapper = (Mapper) mapper_class.newInstance();
		Path input = new Path(props.getProperty("INPUT_PATH"));
		Path output = new Path(props.getProperty("OUTPUT_PATH"));

		Context context=new Context();
		
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.80.11:9000"), new Configuration(), "root");
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(input,false);
		
		while(iter.hasNext()) {
			LocatedFileStatus file = iter.next();
			FSDataInputStream in = fs.open(file.getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while ((line=br.readLine())!=null) {
				mapper.map(line, context);
			}
			br.close();
			in.close();
		}
		
		if(fs.exists(output)) {
			throw new RuntimeException("指定的输出目录已经存在，请更换。。。。。。!");
		}
		
		HashMap<Object, Object> contextMap = context.getContextMap();
		FSDataOutputStream out = fs.create(new Path(props.getProperty("OUTPUT_PATH"), new Path("res.dat")));
		Set<Entry<Object, Object>> entrySet = contextMap.entrySet();
		for (Entry<Object, Object> entry : entrySet) {
			out.write((entry.getKey().toString()+'\t'+entry.getValue().toString()+'\n').getBytes());
		}
		out.close();
		fs.close();
		System.out.println("恭喜！数据统计完成..........");
	}
}
