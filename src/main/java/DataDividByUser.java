import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DataDividByUser {
    //input: [1,10001,5.0]
    //output: [1    10001:5.0,10002:2.3...]
    public static class ReadData extends Mapper<LongWritable, Text, Text, Text> {
        //input: [1,10001,5.0]
        //output: [1    10001:5.0]
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //line[] = [1,10001,5.0]
            String[] line = value.toString().trim().split(",");
            if(line.length < 3) {
                return;
            }
            context.write(new Text(line[0].trim()), new Text(line[1].trim() + ":" + line[2].trim()));
        }
    }

    public static class MergeData extends Reducer<Text, Text, Text, Text> {
        //input: key:"1", value:<"10001:5.0", "10002:4.5"...>
        //output: key:"1", value:"10001:5.0,10002:4.5...";
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

            StringBuilder s = new StringBuilder();
            for (Text elem : value) {
                s.append(elem.toString().trim());
                s.append(",");
            }
            s.	deleteCharAt(s.length() - 1);
            context.write(new Text(key), new Text(s.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(DataDividByUser.class);
        job.setMapperClass(ReadData.class);
        job.setReducerClass(MergeData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }

}