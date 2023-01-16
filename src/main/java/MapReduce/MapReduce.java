/*
 * package MapReduce;
 * 
 * import java.io.IOException;
 * 
 * import org.apache.accumulo.core.client.AccumuloClient;
 * import org.apache.accumulo.core.data.Mutation;
 * import org.apache.accumulo.core.data.Value;
 * import org.apache.accumulo.examples.Common;
 * import org.apache.accumulo.examples.cli.ClientOpts;
 * import org.apache.accumulo.hadoop.mapreduce.AccumuloOutputFormat;
 * import org.apache.hadoop.fs.Path;
 * import org.apache.hadoop.io.LongWritable;
 * import org.apache.hadoop.io.Text;
 * import org.apache.hadoop.mapreduce.Job;
 * import org.apache.hadoop.mapreduce.Mapper;
 * import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
 * 
 * public class MapReduce {
 * 
 * static class NGramMapper extends Mapper<LongWritable, Text, Text, Mutation> {
 * 
 * @Override
 * protected void map(LongWritable location, Text value, Context context)
 * throws IOException, InterruptedException {
 * String[] parts = value.toString().split("\\t");
 * if (parts.length >= 4) {
 * Mutation m = new Mutation(parts[0]);
 * m.put(parts[1], String.format("%010d", Long.parseLong(parts[2])),
 * new Value(parts[3].trim().getBytes()));
 * context.write(null, m);
 * }
 * }
 * }
 * 
 * }
 */
