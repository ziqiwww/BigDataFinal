package WebAnalyser;


import java.io.DataInput;
import java.io.DataOutput;
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

public class AddressCnt {

    private static class RequestAddressPair implements WritableComparable<RequestAddressPair> {
        private Text request;
        private Text address;

        public RequestAddressPair() {
            this.request = new Text();
            this.address = new Text();
        }

        public RequestAddressPair(String request, String address) {
            this.request = new Text(request);
            this.address = new Text(address);
        }

        public void set(String request, String address) {
            this.request.set(request);
            this.address.set(address);
        }

        public Text getRequest() {
            return request;
        }

        public Text getAddress() {
            return address;
        }

        @Override
        public void write(DataOutput out) throws java.io.IOException {
            request.write(out);
            address.write(out);
        }

        @Override
        public void readFields(DataInput in) throws java.io.IOException {
            request.readFields(in);
            address.readFields(in);
        }

        @Override
        public int compareTo(RequestAddressPair o) {
            int cmp = request.compareTo(o.request);
            if (cmp != 0) {
                return cmp;
            }
            return address.compareTo(o.address);
        }

        @Override
        public String toString() {
            return request + "\t" + address;
        }
    }

    private static class AddressCntMapper extends Mapper<LongWritable, Text, RequestAddressPair, IntWritable> {
        IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            String line = value.toString();
            AccessParser parser = new AccessParser(line);
            if (parser.parse()) {
                String addressStr = parser.getRemote_addr();
                String requestStr = parser.getRequest().split(" ")[1];
                context.write(new RequestAddressPair(requestStr, addressStr), one);
            }
        }
    }

    private static class AddressCntPartitioner extends Partitioner<RequestAddressPair, IntWritable> {
        @Override
        public int getPartition(RequestAddressPair key, IntWritable value, int numPartitions) {
            return (key.getRequest().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    private static class AddressCntReducer extends Reducer<RequestAddressPair, Text, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        private String prevRequest = "";
        private String prevAddress = "";
        private int cnt = 0;

        public void reduce(RequestAddressPair key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
            String request = key.getRequest().toString();
            String address = key.getAddress().toString();
            if (request.equals(prevRequest)) {
                if (!address.equals(prevAddress)) {
                    cnt++;
                }
            } else {
                if (!prevRequest.equals("")) {
                    result.set(cnt);
                    context.write(new Text(prevRequest), result);
                }
                prevRequest = request;
                prevAddress = address;
                cnt = 1;
            }
        }

        @Override
        public void cleanup(Context context) throws java.io.IOException, InterruptedException {
            if (!prevRequest.equals("")) {
                result.set(cnt);
                context.write(new Text(prevRequest), result);
            }
        }
    }

    public static int main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AddressCnt");
        job.setJarByClass(AddressCnt.class);
        job.setMapperClass(AddressCntMapper.class);
        job.setPartitionerClass(AddressCntPartitioner.class);
        job.setReducerClass(AddressCntReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(RequestAddressPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
