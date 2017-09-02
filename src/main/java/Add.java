import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Add {
    //input: key is userID:movieA_ID value is rating * relation
    //output:key is userID value is movieID,rating
    public static class ConvertToDouble extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        //input:[1:1    5]
        //output: key-1 value- 1:5
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split("\\s+");
            if (line.length < 2) {
                return;
            }
            String outputKey = line[0];
            double outputValue = Double.parseDouble(line[1]);
            context.write(new Text(outputKey), new DoubleWritable(outputValue));
        }
    }

    public static class MatrixAdd extends Reducer<Text,DoubleWritable, Text, Text> {
        //input:key 1 value <1:5, 2:10, 3:5...>
        //output:key 1:1 value 5/sum
        public void reduce(Text key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable elem : value) {
                sum += elem.get();
            }
            String[] line = key.toString().trim().split(":");
            String outputKey = line[0];
            String outputValue = line[1] + "," + String.valueOf(sum);
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Add.class);

        job.setMapperClass(ConvertToDouble.class);
        job.setReducerClass(MatrixAdd.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}
