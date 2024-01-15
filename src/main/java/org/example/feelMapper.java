package org.example;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class feelMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        // 获得每行文档内容，进行拆分
        String content[] = data.split("\t", 2);
        // 获取情感标签:content[0]
        String label = content[0];
        // 获取当前行的特征
        String features[] = content[1].split(" ");
        // 清洗数据并统计
        for (String feature : features) {
            // 清洗数据
            if (Pattern.matches("[\u4e00-\u9fa5]+", feature)) {
                // 输出一次该类别下特征计数
                context.write(new Text(label + "_" + feature), one);
            }
        }
        // 输出情感标签
        context.write(new Text(label), one);
    }
}


//import java.io.IOException;
//import java.util.regex.Pattern;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//public class feelMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//    private final IntWritable one = new IntWritable(1);
//
//    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String data = value.toString();
//        // 获得每行文档内容，进行拆分
//        String content[] = data.split("\t", 2);
//        // 获取情感标签:content[0]
//        String label = content[0];
//        // 获取当前行的特征
//        String features[] = content[1].split(" ");
//        // 清洗数据并统计
//        for (String feature : features) {
//            // 清洗数据
//            if (Pattern.matches("[\u4e00-\u9fa5]+", feature)) {
//                // 输出一次该类别下特征计数
//                context.write(new Text(label + "_" + feature), one);
//            }
//        }
//        // 输出情感标签
//        context.write(new Text(label), one);
//    }
//}
