import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Normalize {
    //input:[1:1    5] coMtrix's output
    //output:key 1:1 value 5/sum
    public static class SplitMoviePair extends Mapper<LongWritable, Text, Text, Text> {
        //input:[1:1    5]
        //output: key-1 value- 1:5
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split("\\s+");
            String[] wordOne = line[0].trim().split(":");
            context.write(new Text(wordOne[0].trim()), new Text(wordOne[1].trim() + ":" + line[1].trim()));
        }
    }

    public static class NormalizeRelationship extends Reducer<Text, Text, Text, Text> {
        //input:key 1 value <1:5, 2:10, 3:5...>
        //output:key 1:1 value 5/sum
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            Map<String, Double> map = new HashMap<String, Double>();
            double sum = 0;
            for (Text elem : value) { //1:5
                String[] line = elem.toString().trim().split(":");
                double thisValue = Double.parseDouble(line[1]);
                sum = sum + thisValue;
                map.put(line[0].trim()+":"+ key, thisValue); // 1:2 5 ---> 2:1 5
            }

            for (String elem : map.keySet()) {
                map.put(elem, map.get(elem)/sum);
                context.write(new Text(elem), new Text(String.valueOf(map.get(elem))));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Normalize.class);
        job.setMapperClass(SplitMoviePair.class);
        job.setReducerClass(NormalizeRelationship .class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }

}
