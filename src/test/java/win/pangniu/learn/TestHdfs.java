package win.pangniu.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import win.pangniu.learn.utils.HDFSUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * <p>Date:2017/10/12</p>
 * <p>Module:</p>
 * <p>Description: </p>
 * <p>Remark: </p>
 *
 * @author wuxiangbo
 * @version 1.0
 *          <p>------------------------------------------------------------</p>
 *          <p> Change history</p>
 *          <p> Serial number: date:modified person: modification reason:</p>
 */
public class TestHdfs {


    @Test
    public void testHdfs(){
        long start = System.currentTimeMillis();
        boolean b = HDFSUtils.putToHDFS("E://123.txt", "hdfs://192.168.191.112:9000/12345.txt");
        long end = System.currentTimeMillis();
        System.out.println(end-start);

    }

    @Test
    public void testDownLoad() throws URISyntaxException, IOException {
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.191.112:9000"), new Configuration());
        InputStream in = fs.open(new Path("/breakPointFile/cloudmusicsetup_2.2.0.57309.exe"));
        OutputStream out = new FileOutputStream("E://cloudmusicsetup_2.2.0.57309.exe");
        IOUtils.copyBytes(in, out, 4096, true);
    }

    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) {
            testAppend();
        }
    }



    public static void testAppend(){
        String hdfs_path = "hdfs://192.168.191.112:9000/12345.txt";//文件路径
        Configuration conf = new Configuration();
        Path path = new Path(hdfs_path);
        conf.setBoolean("dfs.support.append", true);
//        conf .set("dfs.client.block.write.replace-datanode-on-failure.policy" ,"NEVER" );
//        conf .set("dfs.client.block.write.replace-datanode-on-failure.enable" ,"true" );
        String inpath = "E://123.txt";
        FileSystem fs = null;
        try {
            fs = path.getFileSystem(conf);
            boolean exists = fs.exists(path);
            if(!exists){
                fs.create(new Path(hdfs_path)).close();
            }


            fs = FileSystem.get(URI.create(hdfs_path), conf);
            //要追加的文件流，inpath为文件
            InputStream in = new
                    BufferedInputStream(new FileInputStream(inpath));
            OutputStream out = fs.append(new Path(hdfs_path));
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
