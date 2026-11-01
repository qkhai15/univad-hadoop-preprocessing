import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

public class SplitAndMetaJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SplitAndMetaJob <input_list_txt> <output_dir>");
            System.err.println("Received args: " + String.join(" ", args));
            return 1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Split and Meta JSON Generator");

        job.setJarByClass(SplitAndMetaJob.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // BỎ QUA TÊN CLASS NẾU HADOOP CHÈN VÀO
        if (otherArgs.length > 0 && otherArgs[0].contains("SplitAndMetaJob")) {
            String[] filtered = new String[otherArgs.length - 1];
            System.arraycopy(otherArgs, 1, filtered, 0, filtered.length);
            otherArgs = filtered;
        }

        int res = ToolRunner.run(conf, new SplitAndMetaJob(), otherArgs);
        System.exit(res);
    }
}