package org.apache.hadoop.hdfs.server.mytest_namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * todo FileSystem 类是通过配置参数匹配到fs.defaultFS 对应的FileSystem实现类. 然后由实现类来进行操作.
 *  举例 : "hdfs://master"匹配的DistributedFileSystem实现类.然后DistributedFileSystem的实现类里面封装了 DFSClient 类. 然后后面的操作都是通过DFSClient与hadoop进行通讯.

 */
public class Test_hdfs {
    public static void main(String[] args) throws IOException {
        //构建配置 , 默认加载core-site.xml hdfs-site.xml
        Configuration conf = new Configuration();

        //构建FileSystem   [核心]
        FileSystem  fs = FileSystem.get(conf);

        //读取根目录上有哪些文件.
        FileStatus[] list = fs.listStatus(new Path("/"));
        for (FileStatus file:list ) {
            System.out.println(file.getPath().getName());
        }
    }
}
