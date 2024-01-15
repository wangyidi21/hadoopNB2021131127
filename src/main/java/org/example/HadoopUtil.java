package org.example;




//import cn.hutool.log.Log;
//import cn.hutool.log.LogFactory;
//import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;


import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;


public class HadoopUtil {
//    private final Log log = LogFactory.get(Log4j2.class);

    private final String defaultFS="hdfs://192.168.31.228:39000";
    private FileSystem fileSystem;
    private final  String hadoopHome="D:\\HadoopServlet";
    private FileSystem fileSystemInit() throws IOException, URISyntaxException, InterruptedException {
//        String hadoopHome = "D:\\HadoopServlet";
        String hadoop = System.getenv("HADOOP_HOME");//获取环境变量,判断当前环境是否有Hadoop配置,有的话就直接用HADOOP_HOME的变量，否则就是配置文件中的HADOOP_HOME
        if (StringUtil.isNotEmpty(hadoop)) System.setProperty("hadoop.home.dir", hadoop);
        else System.setProperty("hadoop.home.dir", hadoopHome);
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        conf.set("dfs.replication", "1");
        URI uri=new URI(defaultFS);
        conf.set("fs.defaultFS", defaultFS);
        FileSystem fileSystem = FileSystem.get(uri,conf,"root");
//        log.info("fileSystem创建成功");
        return fileSystem;
//        return null;
    }

    public HadoopUtil() {
        try {
            this.fileSystem=fileSystemInit();
        } catch (IOException | URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param srcPath:本地文件路径
     * @param dstPath:HDFS中的文件路径
     * @return 上传结果
     */
    public boolean uploadFile(String srcPath, String dstPath) {
        Path src = new Path(srcPath);
        Path dst = new Path(defaultFS + dstPath);
        try {
            fileSystem.copyFromLocalFile(false, true, src, dst);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * @param FileName:上传到Hadoop的HDFS文件路径名字(包含路径)
     * @param data:byte流数据
     */
    public void uploadFile(String FileName, byte[] data) {
        Path dstPath = new Path(FileName);
        FSDataOutputStream out = null;
        try {
            out = fileSystem.create(dstPath, true);
            out.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            CloseStream(out);
        }
    }

    public void CloseFileSystem() {
        try {
            if (fileSystem != null) {
                fileSystem.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param srcPath:远程文件路径
     * @param dstPath:本地文件路径路径
     * @return 下载结果
     */
    public boolean downloadFile(String srcPath, String dstPath) {
        Path src = new Path(srcPath);
        Path dst = new Path(dstPath);
        try {
            fileSystem.copyToLocalFile(false, src, dst, true);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean deleteFile(String dstPath) {
        if (!fileExists(dstPath)) return false;
        try {
            fileSystem.delete(new Path(dstPath), true);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * @param outputStream:要下载到的输出流
     * @param dstPath:在服务器中的地址,前提是文件要存在
     */
    public void downloadFile(OutputStream outputStream, String dstPath) {
        Path downLoadPath = new Path(dstPath);
        FSDataInputStream in = null;
        try {
            in = fileSystem.open(downLoadPath);
            IOUtils.copyBytes(in, outputStream, 4096, false);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            CloseStream(in);
        }
    }

    public boolean fileExists(String path) {
        Path file = new Path(defaultFS + path);
        try {
            return fileSystem.exists(file);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * @param distPath:文件上传的路径
     * @param text:上传的文本内容
     */
    public void appendStringToFile(String distPath, String text) {
        Path appendPath = new Path(distPath);
        FSDataOutputStream out = null;
        try {
            out = fileSystem.append(appendPath);
            String appendStr = text + "\r\n";
            out.write(appendStr.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            CloseStream(out);
        }
    }

    public List<String > readResult(String path) {

        List<String> result=new ArrayList<>();
        // 检查文件是否存在
        Path filePath = new Path(path);
        try {
            if (!fileSystem.exists(filePath)) {
                throw new IOException("文件不存在：" + path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 读取文件内容
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)));
            String line;
            while (StringUtil.isNotEmpty(line = br.readLine())) {
                result.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     *
     * @param path:文件在HDFS中的路径
     * @param maxTime: 最大存活时间
     * @param containReadTime: 是否需要考虑上次读的时间
     * @return
     */

    public boolean FileIsInTime(String path,long maxTime,boolean containReadTime){
        try {
            boolean exists = fileExists(path);
            if (!exists) return false;
            FileStatus status = fileSystem.getFileStatus(new Path(path));
            long createTime = containReadTime? minMaxTime(status.getModificationTime(), status.getAccessTime(),maxTime):status.getModificationTime();
            long currentTimeMillis = System.currentTimeMillis();
            return (currentTimeMillis-createTime)< maxTime;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public boolean canFlush(String path,long minTime){
        try {
            boolean exists = fileExists(path);
            if (!exists) return true;
            FileStatus status = fileSystem.getFileStatus(new Path(path));
            long createTime =status.getModificationTime();
            long currentTimeMillis = System.currentTimeMillis();
            return (currentTimeMillis-createTime) < minTime;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    //防止多次读导致的缓存与实际的时间差过大
    private long minMaxTime(long modificationTime,long accessTime,long maxTime){
        long max = Math.max(accessTime - modificationTime, modificationTime - accessTime);
        return max>maxTime ?Math.min(modificationTime,accessTime):Math.max(modificationTime,accessTime);
    }



    private void CloseStream(Closeable stream) {

        try {
            if (stream != null) {
                stream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // FSDataInputStream in = fileSystem.open(appendPath);
    //            long len = fileSystem.getFileStatus(appendPath).getLen();
    //            // 打开输出流并进行追加操作
    //            FSDataOutputStream out = fileSystem.create(appendPath, true);
    //            out.write(text.getBytes(), (int) len,text.getBytes().length);
    //            out.writeBytes(text);
    //            // 关闭输入流和输出流
    //            in.close();


}