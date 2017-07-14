import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;

public class SortDriver extends Configured{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
//		ToolRunner.run(new SortDriver(), args);
		JobConf conf = new JobConf(SortDriver.class);
		conf.setJobName("Mapred");
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setMapperClass(MapperSort.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setReducerClass(IdentityReducer.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		JobClient.runJob(conf);
	}

//	@Override
//	public int run(String[] arg0) throws Exception {
//		// TODO Auto-generated method stub
//		JobConf conf = new JobConf(SortDriver.class);
//		conf.setJobName(this.getClass().getName());
//		FileInputFormat.setInputPaths(conf, new Path(arg0[0]));
//		FileOutputFormat.setOutputPath(conf, new Path(arg0[1]));
//		
//		conf.setMapperClass(MapperSort.class);
//		conf.setMapOutputKeyClass(Text.class);
//		conf.setReducerClass(IdentityReducer.class);
//		conf.setMapOutputValueClass(Text.class);
//		
//		JobClient.runJob(conf);
//		return 0;
//	}

}
