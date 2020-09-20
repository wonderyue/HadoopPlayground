import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MaxCommonFriends {

    public static class MaxCommonFriendMapper
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

    public static class MaxCommonFriendReducer
            extends Reducer<Text, Text, Text, IntWritable> {

        private Text maxKey = new Text();
        private IntWritable maxValue = new IntWritable(0);

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<Integer> result = null;
            for (Text val : values) {
                String[] friends = val.toString().split(",");
                Set<Integer> tmp = new HashSet<>();
                for (String s : friends) {
                    tmp.add(Integer.valueOf(s));
                }
                if (result == null) {
                    result = tmp;
                } else {
                    result.retainAll(tmp);
                }
            }
            if (result != null && result.size() > maxValue.get()) {
                maxKey.set(key);
                maxValue.set(result.size());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            context.write(maxKey, maxValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max common friends");
        job.setJarByClass(MaxCommonFriends.class);
        job.setMapperClass(MaxCommonFriendMapper.class);
        job.setReducerClass(MaxCommonFriendReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}