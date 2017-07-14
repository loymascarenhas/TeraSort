import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapperSort extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable arg0, Text arg1, OutputCollector<Text, Text> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		arg2.collect(new Text(arg1.toString().substring(0, 10)), new Text(arg1.toString().substring(10, 98)));
		
	}

}
