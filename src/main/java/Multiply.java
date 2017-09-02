import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.HashMap;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.Map;

public class Multiply {
    //input: Normalize output and GetRatingList output
    //output:
    public static class ReadNormalizedUnit extends Mapper<Object, Text, Text, Text> {
        //input: [2(movieB):1(movieA)    0.57(rating)]
        //output: [2(movieB)    1:0.57(movieA:rating)
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split("\\s+");
            if (line.length < 2) {
                return;
            }
            String outputKey = line[0].split(":")[0].trim();
            String outputValue = line[0].split(":")[1].trim() + "=" + line[1].trim();
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class ReadRatingUnit extends Mapper<Object, Text, Text, Text> {
        //input: [10001(movieID)    1(UserID):5.0(rating)]
        //output: output same as input
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split("\\s+");
            if (line.length < 2) {
                return;
            }
            context.write(new Text(line[0].trim()), new Text(line[1].trim()));
        }
    }

    public static class MultiplyUnit extends Reducer<Text, Text, Text, Text> {
        //intput: key is movieB_ID value = <movieA_ID=relation, userID:rating>
        //output: key is userID:movieA_ID value = rating * relation
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            Map<String, Double> movieMap = new HashMap<String, Double>();
            Map<String, Double> usersMap = new HashMap<String, Double>();
            for (Text elem : value) {
                if (elem.toString().contains("=")) { //MAYBE NO HERE!
                    String[] line = elem.toString().trim().split("=");
                    movieMap.put(line[0], Double.parseDouble(line[1]));
                } else if (elem.toString().contains(":")) {
                    String[] line2 = elem.toString().trim().split(":");
                    usersMap.put(line2[0], Double.parseDouble(line2[1]));
                }
            }
            for (String movieID : movieMap.keySet()) {
                for (String userID : usersMap.keySet()) {
                    String outputKey = userID + ":" + movieID;
                    double outputValue = movieMap.get(movieID) * usersMap.get(userID);
                    context.write(new Text(outputKey), new Text(String.valueOf(outputValue)));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Multiply.class);

        ChainMapper.addMapper(job, ReadNormalizedUnit.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, ReadRatingUnit.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplyUnit.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReadNormalizedUnit.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReadRatingUnit.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
