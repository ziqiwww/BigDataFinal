package WebAnalyser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AgentType {

    private static class AgentTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text agentType = new Text();

        public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            String line = value.toString();
            AccessParser parser = new AccessParser(line);
            if (parser.parse()) {
                agentType.set(parser.getHttp_user_agent());
                context.write(agentType, one);
            }
        }
    }

    private static class AgentTypeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws java.io.IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
//            if (sum > 1000) {
            context.write(key, result);
//            }
        }
    }

//    public static class AgentTypePartitioner extends Partitioner<Text, IntWritable> {
//        @Override
//        public int getPartition(Text key, IntWritable value, int numPartitions) {
//            String agent = key.toString();
//            if (agent.contains("Chrome")) {
//                return 0;
//            } else if (agent.contains("Firefox")) {
//                return 1;
//            } else if (agent.contains("Safari")) {
//                return 2;
//            } else if (agent.contains("Trident")) {
//                return 3;
//            } else {
//                return 4;
//            }
//        }
//    }

    public static int main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AgentType");
        job.setJarByClass(AgentType.class);
        job.setMapperClass(AgentTypeMapper.class);
        job.setCombinerClass(AgentTypeReducer.class);
        job.setReducerClass(AgentTypeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
