package org.bd.hdfs;

import org.apache.hadoop.fs.FileStatus;



public class Test {
	
	public static void main(String[] args) {
		HdfsClient item = new HdfsClient();
		/*boolean flag1 = item.isDirExists("/user/udf_prog/idata-udf.jar/idata-udf.jar");
		if(flag1){
			System.out.println("路径存在");
		}else{
			System.out.println("路径不存在");
		}*/
		/*boolean falg2 = item.mkDir("/user/udf_prog/output/test1/test2");
		if(falg2){
			System.out.println("创建成功");
		}else{
			System.out.println("创建失败");
		}*/
		/*boolean flag3 = item.deleteDir("/user/udf_prog/output/testFold/");
		if(flag3){
			System.out.println("删除成功");
		}else{
			System.out.println("删除失败");
		}*/
		/*FileStatus[] list = item.dirList("/user/udf_prog");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d, %s, %s, %d\n", f.getPath(), f.isDirectory(), f.getLen(), f.getGroup(), f.getOwner(), f.getBlockSize());
        }*/
        /*boolean flag4 = item.renameDir("/user/udf_prog/output/test11", "/user/udf_prog/output/test1");
        if(flag4){
        	System.out.println("重命名成功");
        }else{
        	System.out.println("重命名失败");
        }*/
//        item.uploadFile("C:\\Users\\Administrator\\Desktop/idata.sql", "/user/udf_prog/output/test1");
        item.downloadFile("C:\\Users\\Administrator\\Desktop\\ceshi", "/user/udf_prog/output/test1/test.sql");
//		item.createFile("/user/udf_prog/output/test1/test.txt", "fsadfsadfsadfsadf");
		/*boolean flag3 = item.deleteDir("/user/udf_prog/output/test1/test.txt");
		if(flag3){
			System.out.println("success");
		}else{
			System.out.println("fail");
		}*/
		
		/*String look = item.lookFile("/user/udf_prog/output/test1/test.sql");
		System.out.println(look);*/
		
		/*FileStatus[] paths = item.dirListByLike("/user/udf_prog/output/test1","t");
		for(FileStatus p : paths){
			System.out.println(p.getPath());
		}*/
		System.exit(0);
	}

}
