package WebAnalyser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DeviceType {

    private static class RequestDevicePair implements WritableComparable<RequestDevicePair> {
        private Text request;
        private Text device;

        public RequestDevicePair() {
            this.request = new Text();
            this.device = new Text();
        }

        public RequestDevicePair(String request, String device) {
            this.request = new Text(request);
            this.device = new Text(device);
        }

        public void set(String request, String device) {
            this.request.set(request);
            this.device.set(device);
        }

        public Text getRequest() {
            return request;
        }

        public Text getDevice() {
            return device;
        }

        @Override
        public void write(DataOutput out) throws java.io.IOException {
            request.write(out);
            device.write(out);
        }

        @Override
        public void readFields(DataInput in) throws java.io.IOException {
            request.readFields(in);
            device.readFields(in);
        }

        @Override
        public int compareTo(RequestDevicePair o) {
            int cmp = request.compareTo(o.request);
            if (cmp != 0) {
                return cmp;
            }
            return device.compareTo(o.device);
        }
    }

    private static class RequestDeviceMapper extends Mapper<LongWritable, Text, RequestDevicePair, IntWritable> {
        private final RequestDevicePair requestDevicePair = new RequestDevicePair();
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            AccessParser parser = new AccessParser(value.toString());
            if (parser.parse()) {
                String requestStr = parser.getRequest().split(" ")[1];
                requestDevicePair.set(requestStr, parser.getDevice());
                context.write(requestDevicePair, one);
            }
        }
    }

    private static class RequestDevicePartitioner extends Partitioner<RequestDevicePair, IntWritable> {
        @Override
        public int getPartition(RequestDevicePair key, IntWritable value, int numPartitions) {
            return (key.getRequest().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    private static class RequestDeviceReducer extends Reducer<RequestDevicePair, IntWritable, Text, Text> {

        private String prevRequest = "";
        private String prevDevice = "";

        private StringBuilder postingList = new StringBuilder();

        int count = 0;

        @Override
        protected void reduce(RequestDevicePair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String requestStr = key.getRequest().toString();
            String deviceStr = key.getDevice().toString();

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if(requestStr.equals(prevRequest)) {
                if(deviceStr.equals(prevDevice)) {
                    count += sum;
                }else{
                    postingList.append(prevDevice).append(":").append(count).append(",");
                    count = sum;
                    prevDevice = deviceStr;
                }
            } else {
                if(postingList.length() > 0) {
                    postingList.deleteCharAt(postingList.length() - 1);
                    context.write(new Text(prevRequest), new Text(postingList.toString()));
                }
                prevRequest = requestStr;
                prevDevice = deviceStr;
                postingList = new StringBuilder();
                count = sum;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String postingListStr = postingList.toString();
            if (postingListStr.length() > 0) {
                postingListStr = postingListStr.substring(0, postingListStr.length() - 1);
                context.write(new Text(prevRequest), new Text(postingListStr));
            }
        }
    }

    public static int main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DeviceType");
        job.setJarByClass(DeviceType.class);
        job.setMapperClass(RequestDeviceMapper.class);
        job.setPartitionerClass(RequestDevicePartitioner.class);
        job.setReducerClass(RequestDeviceReducer.class);
        job.setMapOutputKeyClass(RequestDevicePair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
