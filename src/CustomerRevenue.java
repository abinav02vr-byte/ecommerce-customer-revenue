import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomerRevenue {

    public static class RevenueMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final Text customerId = new Text();
        private final DoubleWritable revenue = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            // Skip the CSV header and empty lines
            if (line.isEmpty() || line.startsWith("customer_id,")) {
                return;
            }

            String[] fields = line.split(",");

            if (fields.length != 4) {
                return;
            }

            try {
                String customer = fields[0].trim();
                int quantity = Integer.parseInt(fields[2].trim());
                double price = Double.parseDouble(fields[3].trim());

                double total = quantity * price;

                customerId.set(customer);
                revenue.set(total);

                context.write(customerId, revenue);

            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid row: " + line);
            }
        }
    }

    public static class RevenueReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private final DoubleWritable totalRevenue = new DoubleWritable();

        @Override
        public void reduce(Text customer, Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;

            for (DoubleWritable value : values) {
                sum += value.get();
            }

            totalRevenue.set(sum);
            context.write(customer, totalRevenue);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: CustomerRevenue <input path> <output path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Wise Revenue");

        job.setJarByClass(CustomerRevenue.class);
        job.setMapperClass(RevenueMapper.class);
        job.setReducerClass(RevenueReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}