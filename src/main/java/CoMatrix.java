import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CoMatrix {
    //input: [1  1:10,2:2.5,3:8...] data_divider's output
    //output: key[1:1] ,value<5>
    public static class SplitMovie extends Mapper<LongWritable, Text, Text, IntWritable> {
        //input:[1  1:10,2:2.5,3:8...]
        //output:key 1:1 value 1
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //line[1:10, 2:2.5, 3:8 ...]
            String[] line = value.toString().trim().split("\\s+")[1].trim().split(",");
            for (int i = 0; i < line.length; i++) {
                line[i] = line[i].trim().split(":")[0];
            }

            for (int i = 0;i < line.length; i++) {
                for (int j = 0; j < line.length; j++) {
                    context.write(new Text(line[i] + ":" + line[j]), new IntWritable(1));
                }
            }

        }
    }

    public static class MergeMovie extends Reducer<Text, IntWritable, Text, IntWritable> {
        //input:key[1:1] value:<1,1,1,1,1>
        //output:key[1:1] ,value<5>
        @Override
        public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {

            int sum  = 0;

            for(IntWritable elem : value) {
                sum += elem.get();
            }

            context.write(new Text(key), new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(CoMatrix.class);

        job.setMapperClass(SplitMovie.class);
        job.setReducerClass(MergeMovie.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

}