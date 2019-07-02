import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;


public class HDFSUtils {
	private static final String HDFS = "hdfs://192.168.125.5:9000/";

	private static final Configuration configuration = new Configuration();

	/**
	 * @param folder dir path
	 */
	public static void mkdir(String folder) throws Exception {
		Path path = new Path(folder);
		FileSystem fileSystem = FileSystem.get(URI.create(HDFS), configuration);
		if (!fileSystem.exists(path)) {
			fileSystem.mkdirs(path);
			System.out.println("Created folder: " + folder + "!");
		}
		else {
		    System.out.println("Created folder failed.");
		}
		fileSystem.close();
	}

	/**
	 * @param folder dir to delete
	 */
	public static void rmr(String folder) throws Exception {
		Path path = new Path(folder);
		FileSystem fileSystem = FileSystem.get(URI.create(HDFS), configuration);
		if (fileSystem.deleteOnExit(path)) {
		    System.out.println("delete " + folder + " success.");
		}
		else {
		    System.out.println("delete failed.");
        }
	}

    /**
     * 重命名文件
     * @param src 源文件名
     * @param dst 目标文件名
     * */
    public static void rename(String src, String dst) throws IOException {
        Path name1 = new Path(src);
        Path name2 = new Path(dst);
        FileSystem fs = FileSystem.get(URI.create(HDFS), configuration);
        fs.rename(name1, name2);
        System.out.println("Rename: from " + src + " to " + dst);
        fs.close();
    }

    /**
     * 列出该路径的文件信息
     *
     * @param folder 文件夹名
     */
    public static void ls(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(HDFS), configuration);
        FileStatus[] list = fs.listStatus(path);
        System.out.println("ls: " + folder);
        System.out.println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDirectory(), f.getLen());
        }
        System.out.println("==========================================================");
        fs.close();
    }

    /**
     * 创建文件
     *
     * @param file    文件名
     * @param content 文件内容
     */
    public static void createFile(String file, String content) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(HDFS), configuration);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            System.out.println("Create: " + file);
        } finally {
            if (os != null)
                os.close();
        }
        fs.close();
    }

    /**
     * 复制本地文件到hdfs
     *
     * @param local  本地文件路径
     * @param remote hdfs目标路径
     */
    public static void copyFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(HDFS), configuration);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }

    /**
     * 从hdfs下载文件到本地
     *
     * @param remote hdfs文件路径
     * @param local  本地目标路径
     */
    public static void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(HDFS), configuration);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }

    /**
     * 查看hdfs文件内容
     *
     * @param remoteFile hdfs文件路径
     */
    public static void cat(String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(HDFS), configuration);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + remoteFile);
        try {
            fsdis = fs.open(path);
            IOUtils.copyBytes(fsdis, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
    }

}
