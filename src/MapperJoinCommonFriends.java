import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class MapperJoinCommonFriends {

    public static class MapperJoinCommonFriendMapper
            extends Mapper<Object, Text, Text, Text> {

        private Map<String, String[]> map;
        private int userA;
        private int userB;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            userA = context.getConfiguration().getInt("user_a",0);
            userB = context.getConfiguration().getInt("user_b",0);
            map = new HashMap<>();
            URI[] localPaths = context.getCacheFiles();
            for (URI uri : localPaths) {
                FileSystem fileSystem = FileSystem.get(context.getConfiguration());
                Path path = new Path(uri.getPath());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
                String line;
                while((line = bufferedReader.readLine()) != null){
                    String[] arr = line.split(",");
                    map.put(arr[0], arr);
                }
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] arr = value.toString().split("\t");
            if (arr.length < 2) return;
            int me = Integer.parseInt(arr[0]);
            if (me != userA && me != userB)
                return;

            String[] friends = arr[1].split(",");
            StringBuilder sb = new StringBuilder();
            for (String s: friends) {
                String[] tmp = map.get(s);
                sb.append(tmp[1]);//tmp[1]: firstname
                sb.append(' ');
                sb.append(tmp[2]);//tmp[2]: lastname
                sb.append(':');
                sb.append(tmp[tmp.length-1]);//tmp[arr.length-1]: DOB
                sb.append(',');
            }
            if (sb.length() > 0)
                sb.setLength(sb.length() - 1);
            for (String s : friends) {
                int friend = Integer.valueOf(s);
                if (friend == userA || friend == userB)
                    context.write(new Text(Math.min(me, friend)+","+Math.max(me, friend)), new Text(sb.toString()));
            }
        }
    }

    public static class MapperJoinCommonFriendReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
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
        conf.setInt("user_a", Integer.parseInt(args[3]));
        conf.setInt("user_b", Integer.parseInt(args[4]));
        Job job = Job.getInstance(conf, "mapper join common friends");
        job.setJarByClass(MapperJoinCommonFriends.class);
        job.setMapperClass(MapperJoinCommonFriendMapper.class);
        job.setReducerClass(MapperJoinCommonFriendReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.addCacheFile(new Path(args[2]).toUri());
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}