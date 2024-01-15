package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class feelJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "feelmodel");
        job.setJarByClass(feelJob.class);
        job.setMapperClass(feelMapper.class);
        job.setCombinerClass(feelReducer.class);
        job.setReducerClass(feelReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat
                .setInputPaths(job, new Path("hdfs://localhost:9000/input_2021131127/training.txt"));
        FileOutputFormat.setOutputPath(job, new Path(
                "hdfs://localhost:9000/myfolder/2021131127_模型"));

        int isSuccessed = job.waitForCompletion(true) ? 0 : 1;
        if (isSuccessed == 0) {
            System.out.println("执行成功");
            fileSystem.copyToLocalFile(new Path(
                    "hdfs://localhost:9000/myfolder/2021131127_模型/part-r-00000"), new Path("E:\\study\\hadoop\\output2\\2021131127_模型.txt"));
            System.exit(isSuccessed);
        }else {
           System.out.println("执行失败");

      }

    }
}

//
//import com.util.HadoopUtil;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.example.HadoopUtil;
//import org.example.StringUtil;
////import org.eclipse.jetty.util.StringUtil;
//
//public class feelJob {
//    private static final Configuration conf=getHadoopConfig();
//    protected static HadoopUtil hadoopUtil=new HadoopUtil();
//    private static final String defaultFS="hdfs://192.168.31.228:39000";
//    public static Configuration getHadoopConfig(){
//        Configuration conf = new Configuration();
//        conf.setBoolean("dfs.support.append", true);
//        conf.set("mapreduce.job.run-as-user", "hadoop");
//        conf.set("dfs.replication", "1");
//        conf.set("fs.defaultFS", defaultFS);
//        conf.set("mapred.job.tracker", defaultFS);
////		conf.set("user.name", "root");
////        conf.set("mapreduce.framework.name", "yarn");
////        conf.set("yarn.resourcemanager.address",defaultFS);
//        return conf;
//    }
//    public static void main(String[] args) throws Exception {
////		Configuration conf = new Configuration();
//        String hadoopHome = "D:\\HadoopServlet";
//        String hadoop = System.getenv("HADOOP_HOME");//获取环境变量,判断当前环境是否有Hadoop配置,有的话就直接用HADOOP_HOME的变量，否则就是配置文件中的HADOOP_HOME
//        if (hadoop!=null) {
//            System.setProperty("hadoop.home.dir", hadoop);
//            hadoopHome =hadoop;
//        }
//        else {
//            System.setProperty("hadoop.home.dir", hadoopHome);
//        }
//        if (  System.getProperty("os.name").toLowerCase().contains("win")) System.load(hadoopHome + "/bin/hadoop.dll");
////		conf.set("fs.defaultFS", "hdfs://8.137.13.13:39000");
////		FileSystem fileSystem = FileSystem.get(conf);
//        Job job = Job.getInstance(conf, "MatchJob");
//
////		job.setUser("root");
//        job.setJarByClass(feelJob.class);
//        job.setMapperClass(feelMapper.class);
//        job.setReducerClass(feelReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat
//                .setInputPaths(job, new Path("hdfs://192.168.31.228:39000/input_2021131127/training.txt"));
//        FileOutputFormat.setOutputPath(job, new Path(
//                "hdfs://192.168.31.228:39000/output/2021131127_模型"));
//
//        int isSuccessed = job.waitForCompletion(true) ? 0 : 1;
//        if (isSuccessed == 0) {
//            System.out.println("执行成功");
//            hadoopUtil.downloadFile(
//                    "hdfs://192.168.31.228:9000/output/2021131127_模型/part-r-00000", "2021131127_模型.txt");
//            System.exit(isSuccessed);
//        }else {
//            System.out.println("执行失败");
//            System.out.println(hadoopUtil.deleteFile("/output/2021131127_模型"));
//            hadoopUtil.CloseFileSystem();
//        }
//
//    }
//}
