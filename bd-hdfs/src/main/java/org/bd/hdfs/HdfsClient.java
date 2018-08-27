package org.bd.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
//import java.net.URI;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.bd.hdfs.utils.Consts;
import org.bd.hdfs.utils.SysVarsUtils;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> hdfs客户端<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月15日 上午11:29:54 |创建
 */
public class HdfsClient {
	
	public static void main(String[] arg){
		URI u = URI.create("hdfs://10.10.10.22:8020/");
		System.out.println(u.getAuthority());
		System.out.println(u.getScheme());
	}

    private Configuration conf;
    private String defaultFS;
    private FileSystem fileSystem;
    
    public HdfsClient(){
    	System.setProperty("HADOOP_USER_NAME", "hdfs");//此处应该要动态设置，测试时可以在此处添加，程序运行时，应从系统的环境变量获取
    	this.conf = config();
    	this.defaultFS = SysVarsUtils.getInstance().getVarByName(Consts.FS_DEFAULTFS);
    	try {
			this.fileSystem = FileSystem.get(URI.create(defaultFS), conf);
			Configuration cf = fileSystem.getConf();
			URL u = cf.getResource("hdfs-default.xml");
			System.out.println(u.toString());
			System.out.println(cf.toString());
		} catch (IOException e) {
            String msg = String.format("获取文件系统对象时发生网络IO异常,请检查您的网络是否正常！");
            throw new HdfsClientException(msg);
		}
    }
    
    public JobConf config() {
        JobConf conf = new JobConf(HdfsClient.class);
        conf.setJobName("HdfsClient");
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
//        conf.addResource("classpath:/hadoop/core-site.xml");
//        conf.addResource("classpath:/hadoop/hdfs-site.xml");
//        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }

    /**
     * <b>描述：</b> 判断路径是否存在（可判断文件夹及文件）
     * @author wpk | 2017年11月15日 上午11:45:34 |创建
     * @param filePath
     * @return boolean
     */
    public boolean isDirExists(String filePath) {
        Path path = new Path(filePath);
        boolean exist = false;
        try {
            exist = fileSystem.exists(path);
        } catch (IOException e) {
            String msg = String.format("判断文件路径[%s]是否存在时发生网络IO异常,请检查您的网络是否正常！","filePath =" + filePath);
            throw new HdfsClientException(msg, e);
        }
        return exist;
    }

    /**
     * <b>描述：</b> 删除路径（可删除文件夹及文件）
     * @author wpk | 2017年11月15日 下午12:10:11 |创建
     * @param filePath
     * @return boolean
     */
    public boolean deleteDir(String filePath){
        Path path = new Path(filePath);
        boolean flag = true;
        try {
        	if(fileSystem.exists(path)){
            	flag = fileSystem.delete(path, true);//如果路径为文件夹，则需要设置第二个参数为true,否则将抛出异常；如果路径为文件，则第二个参数可设置true或者false
        	}
        } catch (IOException e) {
            String msg = String.format("删除目录[%s]时发生IO异常,请检查您的网络是否正常！", path.toString());
            throw new HdfsClientException(msg, e);
        }
        return flag;
    }

    /**
     * <b>描述：</b> 获取文件列表信息（包含文件夹及文件）
     * @author wpk | 2017年11月15日 下午2:22:12 |创建
     * @param filePath
     * @return FileStatus[]
     */
    public FileStatus[] dirList(String filePath){
        Path path = new Path(filePath);
        FileStatus[] status = null;
        try {
            status = fileSystem.listStatus(path);
        } catch (IOException e) {
            String msg = String.format("获取目录[%s]文件列表时发生网络IO异常,请检查您的网络是否正常！", filePath);
            throw new HdfsClientException(msg, e);
        }
        return status;
    }
    
    /**
     * <b>描述：</b> 创建文件夹（注意：可以创建多层文件夹）
     * @author wpk | 2017年11月15日 下午2:36:44 |创建
     * @param folder
     * @return boolean
     */
    public boolean mkDir(String folder){
        Path path = new Path(folder);
        boolean flag = true;
        try {
            if (!fileSystem.exists(path)) {
            	flag = fileSystem.mkdirs(path);
            }
        } catch (IOException e) {
            String msg = String.format("创建[%s]文件夹时发生网络IO异常,请检查您的网络是否正常！", folder);
            throw new HdfsClientException(msg, e);
        }
        return flag;
    }
    
    /**
     * <b>描述：</b> 重命名
     * @author wpk | 2017年11月15日 下午3:53:35 |创建
     * @param src
     * @param dst
     * @return boolean
     */
    public boolean renameDir(String src, String dst){
        Path path1 = new Path(src);
        Path path2 = new Path(dst);
        boolean flag = true;
        try {
        	if(!fileSystem.exists(path1)){
        		String msg = String.format("文件路径[%s]不存在", "filePath=" + path1);
        		throw new HdfsClientException(msg);
        	}
        	flag = fileSystem.rename(path1, path2);
		} catch (IOException e) {
            String msg = String.format("重命名文件时发生IO异常,请检查您的网络是否正常！");
            throw new HdfsClientException(msg, e);
		}
        return flag;
    }

    /**
     * <b>描述：</b> 上传文件
     * @author wpk | 2017年11月15日 下午4:50:34 |创建
     * @param local
     * @param remote
     * @return void
     */
    public void uploadFile(String local, String remote){
    	uploadFile(local, remote, false);
    }
    
    /**
     * <b>描述：</b> 上传文件
     * @author wpk | 2017年11月15日 下午4:50:46 |创建
     * @param local
     * @param remote
     * @param deleteLocal	上传后是否删除本地文件
     * @return void
     */
    public void uploadFile(String local, String remote, boolean deleteLocal){
        Path path1 = new Path(local);
        Path path2 = new Path(remote);
        try {
			fileSystem.copyFromLocalFile(deleteLocal, path1, path2);
		} catch (IOException e) {
            String msg = String.format("从本地上传文件至hdfs时发生IO异常,请检查您的网络是否正常！");
            throw new HdfsClientException(msg, e);
		}
    }
    
    /**
     * <b>描述：</b> 下载文件
     * @author wpk | 2017年11月15日 下午4:52:45 |创建
     * @param local
     * @param remote
     * @return void
     */
    public void downloadFile(String local, String remote){
    	downloadFile(local, remote, false);
    }
    
    /**
     * <b>描述：</b> 下载文件
     * @author wpk | 2017年11月15日 下午4:53:42 |创建
     * @param local
     * @param remote
     * @param deleteRemote	下载后是否删除hdfs上的文件
     * @return void
     */
    public void downloadFile(String local, String remote, boolean deleteRemote){
        Path path1 = new Path(remote);
        Path path2 = new Path(local);
        try {
			fileSystem.copyToLocalFile(deleteRemote, path1, path2, true);
		} catch (IOException e) {
            String msg = String.format("从hdfs下载文件至本地时发生IO异常,请检查您的网络是否正常！");
            throw new HdfsClientException(msg, e);
		}
    }
    
    /**
     * <b>描述：</b> 创建文件
     * @author wpk | 2017年11月15日 下午5:02:11 |创建
     * @param filePath	如：hdfs://10.101.204.12:9000/user/hive/test.txt
     * @param content	文件内容
     * @return void
     */
    public void createFile(String filePath, String content){
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
			os = fileSystem.create(new Path(filePath));
            os.write(buff, 0, buff.length);
		} catch (IllegalArgumentException e) {
            throw new HdfsClientException(e);
		} catch (IOException e) {
            String msg = String.format("创建文件[%s]时发生IO异常,请检查您的网络是否正常！", filePath);
            throw new HdfsClientException(msg, e);
		} finally {
	        if (os != null){
				try {
					os.close();//必须调用关闭，才能把管道的内容发送至datanode上
				} catch (IOException e) {
		            throw new HdfsClientException("创建文件关闭流发生IO异常,请检查您的网络是否正常！", e);
				}
	        }
		}
    }

    /**
     * <b>描述：</b> 查看hdfs文件内容，类似linux中的cat命令<br>
     * 不建议读取大文件
     * @author wpk | 2017年11月15日 下午5:12:57 |创建
     * @param remoteFile
     * @return String
     */
    public String lookFile(String remoteFile){
        Path path = new Path(remoteFile);
        FSDataInputStream fsdis = null;
        OutputStream baos = new ByteArrayOutputStream(); 
        String str = null;
        try {
            fsdis = fileSystem.open(path);
            IOUtils.copyBytes(fsdis, baos, 4096, false);
            str = baos.toString();  
        } catch (IOException e) {
            String msg = String.format("查看hdfs文件内容时发生IO异常,请检查您的网络是否正常！");
            throw new HdfsClientException(msg, e);
		} finally {
            IOUtils.closeStream(fsdis);
        }
        return str;
    }
    
    /**
     * <b>描述：</b> 通过模糊查询列表信息
     * @author wpk | 2017年11月15日 下午5:29:21 |创建
     * @param dir	路径
     * @param fileName	查询关键字
     * @return FileStatus[]
     */
    public FileStatus[] dirListByLike(String dir, String fileName){
        Path path = new Path(dir);
        String filterFileName = "*" + fileName + "*";
        FileStatus[] status = null;
        try {
            PathFilter pathFilter = new GlobFilter(filterFileName);
            status = fileSystem.listStatus(path,pathFilter);
        } catch (IOException e) {
            String msg = String.format("获取目录[%s]下文件名包含[%s]的文件列表时发生网络IO异常,请检查您的网络是否正常！", dir,fileName);
            throw new HdfsClientException(msg, e);
        }
        return status;
    }
    
    /**
     * <b>描述：</b> 关闭文件系统
     * @author wpk | 2017年11月15日 下午5:13:46 |创建
     * @return void
     */
    public void closeFS(){
    	try {
			fileSystem.close();
		} catch (IOException e) {
            String msg = String.format("关闭FileSystem时发生IO异常,请检查您的网络是否正常！");
            throw new HdfsClientException(msg, e);
		}
    }
	
}
