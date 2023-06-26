package WebAnalyser;

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

import java.io.DataInput;
import java.io.DataOutput;

public class AccessInHour {
    private static class RequestTimePair implements WritableComparable<RequestTimePair> {
        private final Text request;
        private final Text time;

        public RequestTimePair() {
            this.request = new Text();
            this.time = new Text();
        }

        public RequestTimePair(String request, String time) {
            this.request = new Text(request);
            this.time = new Text(time);
        }

        public void set(String request, String time) {
            this.request.set(request);
            this.time.set(time);
        }

        public Text getRequest() {
            return request;
        }

        public Text getTime() {
            return time;
        }

        @Override
        public void write(DataOutput out) throws java.io.IOException {
            request.write(out);
            time.write(out);
        }

        @Override
        public void readFields(DataInput in) throws java.io.IOException {
            request.readFields(in);
            time.readFields(in);
        }

        @Override
        public int compareTo(RequestTimePair o) {
            int cmp = request.compareTo(o.request);
            if (cmp != 0) {
                return cmp;
            }
            return time.compareTo(o.time);
        }
    }

    private static class RequestTimeMapper extends Mapper<LongWritable, Text, RequestTimePair, IntWritable> {
        private final RequestTimePair requestTimePair = new RequestTimePair();
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            String line = value.toString();
            AccessParser parser = new AccessParser(line);
            if (parser.parse()) {
                String requestStr = parser.getRequest().split(" ")[1];
                String timeStr = parser.getTimeInHour();
                requestTimePair.set(requestStr, timeStr);
                context.write(requestTimePair, one);
            }
        }
    }

    private static class RequestTimePartitioner extends Partitioner<RequestTimePair, IntWritable> {
        @Override
        public int getPartition(RequestTimePair key, IntWritable value, int numPartitions) {
            return (key.getRequest().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    private static class RequestTimeReducer extends Reducer<RequestTimePair, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        String prevRequest = "";
        String prevTime = "";
        int cnt = 0;

        @Override
        protected void reduce(RequestTimePair key, Iterable<IntWritable> values, Context context) throws java.io.IOException, InterruptedException {
            String request = key.getRequest().toString();
            String time = key.getTime().toString();
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (request.equals(prevRequest)) {
                if (time.equals(prevTime)) {
                    cnt += sum;
                } else {
                    result.set(cnt);
                    context.write(new Text(prevRequest + "\t" + prevTime), result);
                    cnt = sum;
                    prevTime = time;
                }
            } else {
                if (!prevRequest.equals("")) {
                    result.set(cnt);
                    context.write(new Text(prevRequest + "\t" + prevTime), result);
                }
                cnt = sum;
                prevRequest = request;
                prevTime = time;
            }
        }

        @Override
        protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
            if (!prevRequest.equals("")) {
                result.set(cnt);
                context.write(new Text(prevRequest + "\t" + prevTime), result);
            }
        }
    }

    public static int main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AccessInHour");
        job.setJarByClass(AccessInHour.class);
        job.setMapperClass(RequestTimeMapper.class);
        job.setPartitionerClass(RequestTimePartitioner.class);
        job.setReducerClass(RequestTimeReducer.class);
        job.setMapOutputKeyClass(RequestTimePair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
