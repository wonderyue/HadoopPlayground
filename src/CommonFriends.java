import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CommonFriends {

    public static class CommonFriendMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] arr = value.toString().split("\t");
            if (arr.length < 2) return;
            int me = Integer.parseInt(arr[0]);
            String[] friends = arr[1].split(",");
            for (String s : friends) {
                int friend = Integer.valueOf(s);
                context.write(new Text(Math.min(me, friend)+","+Math.max(me, friend)), new Text(arr[1]));
            }
        }
    }

    public static class CommonFriendReducer
            extends Reducer<Text, Text, Text, Text> {
        private Set<String> queryPairs;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String s = context.getConfiguration().get("query_pairs");
            String[] arr = s.split(";");
            queryPairs = new HashSet<>(Arrays.asList(arr));
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (!queryPairs.contains(key.toString()))
                return;
            Set<String> result = null;
            for (Text val : values) {
                String[] friends = val.toString().split(",");
                Set<String> tmp = new HashSet<>();
                for (String s : friends) {
                    tmp.add(s);
                }
                if (result == null) {
                    result = tmp;
                } else {
                    result.retainAll(tmp);
                }
            }
            if (result != null) {
                System.out.println(key.toString()+":"+result.toString());
                context.write(key, new Text(result.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("query_pairs", args[2]);//0,1;20,28193;1,29826;6222,19272;28041,28056
        Job job = Job.getInstance(conf, "common friends");
        job.setJarByClass(CommonFriends.class);
        job.setMapperClass(CommonFriendMapper.class);
        job.setReducerClass(CommonFriendReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}