package win.pangniu.learn.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * HDFS的操作工具类
 */
public class HDFSUtils {

	/**
	 * @author dcx by 2015.11.19 新建文件
	 * @param conf
	 * @return
	 */
	public static boolean CreatDir(String dst, Configuration conf) {
		Path dstPath = new Path(dst);
		try {
			FileSystem dhfs = FileSystem.get(conf);
			dhfs.mkdirs(dstPath);
		} catch (IOException ie) {
			ie.printStackTrace();
			return false;
		}
		return true;
	}


    public static boolean createEmptyFile(String dst) {
        Path dstPath = new Path(dst);
        Configuration conf = new Configuration();
        try {
            FileSystem dhfs = dstPath.getFileSystem(conf);
            dhfs.create(dstPath);
            dhfs.close();
        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        }
        return true;
    }


    public static boolean exist(String dst, Configuration conf) {
        Path dstPath = new Path(dst);
        boolean exists = false;
        try {
            FileSystem dhfs = dstPath.getFileSystem(conf);
            exists = dhfs.exists(dstPath);
        } catch (IOException ie) {
            ie.printStackTrace();
        }
        return exists;
    }

    public static boolean exist(String dst) {
        Path dstPath = new Path(dst);
        Configuration conf = new Configuration();

        return exist(dst,conf);
    }


    /**
	 * @author dcx by 2015.11.19 文件上传
	 * @param src
	 * @param dst
	 * @param conf
	 * @return
	 */
	public static boolean putToHDFS(String src, String dst, Configuration conf) {
		Path dstPath = new Path(dst);
		try {
			FileSystem hdfs = dstPath.getFileSystem(conf);
			hdfs.copyFromLocalFile(false, new Path(src), dstPath);
		} catch (Throwable ie) {
            System.out.println(ie.getCause());
            return false;
		}
		return true;
	}

	public static boolean putToHDFS(String src, String dst) {
		Path dstPath = new Path(dst);
        Configuration conf = new Configuration();
		return putToHDFS(src,dst,conf);
	}

	public static boolean append(String path,InputStream input){
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);

        FileSystem fs = null;
        OutputStream out = null;
        try {
            fs = FileSystem.get(URI.create(path), conf);
            //要追加的文件流，inpath为文件
            out = fs.append(new Path(path));
            IOUtils.copyBytes(input, out, 5*1024, true);
        } catch (IOException e) {
            e.printStackTrace();
            return Boolean.FALSE;
        }finally {
            try {
                if(input != null){
                    input.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if(out != null){
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return Boolean.TRUE;
    }

	/**
	 * @author dcx by 2015.11.19 文件下载
	 * @param src
	 * @param dst
	 * @param conf
	 * @return
	 */
	public static boolean getFromHDFS(String src, String dst, Configuration conf) {
		Path srcPath = new Path(src);
		Path dstPath = new Path(dst);

		try {
			FileSystem dhfs = dstPath.getFileSystem(conf);
			dhfs.copyToLocalFile(false, srcPath, dstPath);
		} catch (IOException ie) {
			ie.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * @author dcx by 2015.11.19 文件删除
	 * @param path
	 * @param conf
	 * @return
	 */
	public static boolean checkAndDel(final String path, Configuration conf) {
		Path dstPath = new Path(path);
		try {
			FileSystem dhfs = dstPath.getFileSystem(conf);
			if (dhfs.exists(dstPath)) {
				dhfs.delete(dstPath, true);
			} else {
				return false;
			}
		} catch (IOException ie) {
			ie.printStackTrace();
			return false;
		}
		return true;
	}



	public static FileStatus getFileStatus(final String path) {
		Path dstPath = new Path(path);
		Configuration entries = new Configuration();
		try {
			FileSystem dhfs = dstPath.getFileSystem(entries);
			FileStatus fileLinkStatus = dhfs.getFileLinkStatus(dstPath);
			return fileLinkStatus;
		} catch (Exception ie) {
			return null;
		}
	}

	public static void createFile(final String path, Map<String,String> props,String fsDefaultName) {

		Path dstPath = new Path(path);

		Configuration entries = new Configuration();
		entries.set("fs.default.name", fsDefaultName);
		try {
			FileSystem dhfs = dstPath.getFileSystem(entries);

			Path inFile =new Path(path);

			FSDataOutputStream outputStream=dhfs.create(inFile);

			StringBuffer contentBuffer = new StringBuffer();
			for (String key : props.keySet()){
				String s = key + "=" + props.get(key) + "\n";
				contentBuffer.append(s);
			}
			outputStream.write(contentBuffer.toString().getBytes("UTF-8"));
			outputStream.flush();

			outputStream.close();
		} catch (Exception ie) {
			ie.printStackTrace();
		}
	}

	public static void createFile(final String path, Map<String,String> props) {

		HDFSUtils.createFile(path,props,"hdfs://master.hadoop:9000");

	}
//	/**
//	 * @param 主函数测试
//	 */
//	public static void main(String[] args) {
//
//		boolean status = false;
//		String dst1 = "hdfs://192.168.1.225:9000/EBLearn_data/new";
//		Configuration conf = new Configuration();
//
//		// java.lang.IllegalArgumentException: Wrong FS:
//		// hdfs://192.168.1.225:9000/EBLearn_data/hello.txt, expected: file:///
//		// 解决这个错误的两个方案：
//		// 方案1：下面这条命令必须加上，否则出现上面这个错误
//		conf.set("fs.default.name", "hdfs://192.168.1.225:9000"); // "hdfs://master:9000"
//		// 方案2： 将core-site.xml 和hdfs-site.xml放入当前工程中
//		status = CreatDir(dst1, conf);
//		System.out.println("status=" + status);
//
//		String dst = "hdfs://192.168.1.225:9000/EBLearn_data";
//		String src = "I:/hello.txt";
//
//		status = putToHDFS(src, dst, conf);
//		System.out.println("status=" + status);
//
//		src = "hdfs://192.168.1.225:9000/EBLearn_data/hello.txt";
//		dst = "I:/hadoop_need/";
//		status = getFromHDFS(src, dst, conf);
//		System.out.println("status=" + status);
//
//		dst = "hdfs://192.168.1.225:9000/EBLearn_data/hello.txt";
//		status = checkAndDel(dst, conf);
//		System.out.println("status=" + status);
//	}


	public static void main(String[] args) throws IOException {
//		Path path = new Path("hdfs://192.168.191.112:9000/spark-example/spark-linklink-app-1.0-SNAPSHOT.jar");
//		FileStatus fileStatus = HDFSUtils.getFileStatus("hdfs://192.168.191.112:9000/spark-example/spark-linklink-app-1.0-SNAPSHOT.jar");
//		System.out.println(fileStatus);

		HashMap<String, String> map = new HashMap<>();
		map.put("spark.yarn.cache.visibilities","PUBLIC,PUBLIC");
		map.put("spark.yarn.cache.types","ARCHIVE,FILE");
		map.put("spark.yarn.cache.timestamps","1501905404567,1501483600375");
		map.put("spark.yarn.cache.sizes","186978376,14082");
		map.put("spark.yarn.cache.filenames","hdfs://192.168.191.112:9000/spark-2.2.0-hadoop2.7-dependency.tar.gz\\#__spark_libs__,hdfs\\://192.168.191.112\\:9000/spark-example/spark-linklink-app-1.0-SNAPSHOT.jar\\#__app__.jar");
		String path = "/ttttt/test.properties";
		createFile(path,map);


//		Configuration conf=new Configuration();
//		conf.set("fs.default.name", "hdfs://192.168.191.112:9000");
//		Path inFile =new Path("/spark-example/t1");
//		FileSystem hdfs=FileSystem.get(conf);
//		FSDataOutputStream outputStream=hdfs.create(inFile);
//		outputStream.writeUTF("china cstor cstor china");
//		outputStream.flush();
//		outputStream.close();
	}
}