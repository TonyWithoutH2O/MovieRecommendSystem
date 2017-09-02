import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.net.URI;
import java.util.Map;
import java.util.Set;


public class GetRatingList {
    //input: [1,10001,5.0]
    //output: key: movie_ID \t value: user_ID:rating
    public static class ReadRawData extends Mapper<LongWritable, Text, Text, Text> {
        //input: [1,10001,5.0]
        //output: [10001(movieID)    1(UserID):5.0(rating)]
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //line[] = [1,10001,5.0]
            String[] line = value.toString().trim().split(",");
            if(line.length < 3) {
                return;
            }
            context.write(new Text(line[0].trim()), new Text(line[1]+"="+line[2]));
        }
    }

    public static class MergeMovieList extends Reducer<Text, Text, Text, Text> {

        Set<String> movieList = new HashSet<String>();
        // setup is using to add movielist into a hashset
        public void setup(Context context) throws IOException,  InterruptedException{
            Configuration conf = context.getConfiguration();
            String mList = conf.get("movieList", "");
            Path path = new Path(mList);
            FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop-master:9000"), conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            try {
                String line;
                line=br.readLine();
                while (line != null){
                    movieList.add(line.trim()); //
                    // be sure to read the next line otherwise you'll get an infinite loop
                    line = br.readLine();
                }
            } finally {
                // you should close out the BufferedReader
                br.close();
            }




        }

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            double avg = 0;
            Map<String, String> userMovieList = new HashMap<String, String>();
            for (Text elem : value) {
                String[] line = elem.toString().split("="); //line = [movie_id, rating]
                if (line.length < 2) {
                    return;
                }
                userMovieList.put(line[0].trim(), line[1].trim());
                avg += Double.parseDouble(line[1].trim());
            }

            avg = avg / userMovieList.size();
            for (String movieID : movieList) {
                if (!userMovieList.containsKey(movieID)) {
                    context.write(new Text(movieID), new Text(key.toString() + ":" + String.valueOf(avg)));
                } else {
                    context.write(new Text(movieID), new Text(key.toString() + ":" + userMovieList.get(movieID)));
                }
            }
        }
    }

    public static void main(String[] args) throws  Exception{
        Configuration conf = new Configuration();
        conf.set("movieList",args[1]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(GetRatingList.class);
        job.setMapperClass(ReadRawData.class);
        job.setReducerClass(MergeMovieList.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);

    }

}
