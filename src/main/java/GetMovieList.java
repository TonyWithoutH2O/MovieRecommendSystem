import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GetMovieList {
    //input: [1,10001,5.0]
    //output: 10001,10002.....
    public static class ReadRaw extends Mapper<LongWritable, Text, Text, Text> {
        //input: [1,10001,5.0]
        //output: [1    10001:5.0]
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //line[] = [1,10001,5.0]
            String[] line = value.toString().trim().split(",");
            if(line.length < 3) {
                return;
            }
            context.write(new Text(line[1].trim()), new Text(""));
        }
    }

    public static class MergeUserData extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            String outputKey = key.toString();
            context.write(new Text(outputKey), new Text(""));
        }
    }

    public static void main(String[] args) throws  Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(GetMovieList.class);
        job.setMapperClass(ReadRaw.class);
        job.setReducerClass(MergeUserData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }

}
