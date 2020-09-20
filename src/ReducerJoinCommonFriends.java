import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class ReducerJoinCommonFriends {

    public static class ReducerJoinCommonFriendMapper
            extends Mapper<Object, Text, Text, Text> {

        int year;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            year = Calendar.getInstance().get(Calendar.YEAR);
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.toString().indexOf("\t") != -1) { // friend list
                String[] arr = value.toString().split("\t");
                if (arr.length < 2) return;
                context.write(new Text("list_"+arr[0]), new Text(arr[1]));
            } else { // user data
                String[] arr = value.toString().split(",");
                String[] dob = arr[arr.length-1].split("/");
                int age = year - Integer.parseInt(dob[dob.length-1]);
                context.write(new Text("age_"+arr[0]), new Text(String.valueOf(age)));
            }
        }
    }

    public static class ReducerJoinCommonFriendReducer
            extends Reducer<Text, Text, Text, IntWritable> {

        private Map<String, Integer> id2maxFriendAge = new HashMap<>();
        private Map<String, String[]> id2friendList = new HashMap<>();
        private Map<String, Integer> id2age = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] keyArr = key.toString().split("_");
            String id = keyArr[1];
            if (keyArr[0].equals("list")) {
                id2friendList.put(id, values.iterator().next().toString().split(","));
            } else if (keyArr[0].equals("age")) {
                int age = Integer.parseInt(values.iterator().next().toString());
                id2age.put(id, age);
            }
            if (id2age.containsKey(id) && id2friendList.containsKey(id)) {
                int age = id2age.get(id);
                for (String friendId : id2friendList.get(id)) {
                    int max = id2maxFriendAge.getOrDefault(friendId,0);
                    id2maxFriendAge.put(friendId, Math.max(max, age));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (Map.Entry<String, Integer> kv : id2maxFriendAge.entrySet()) {
                context.write(new Text(kv.getKey()), new IntWritable(kv.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "reducer join common friends");
        job.setJarByClass(ReducerJoinCommonFriends.class);
        job.setMapperClass(ReducerJoinCommonFriendMapper.class);
        job.setReducerClass(ReducerJoinCommonFriendReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}