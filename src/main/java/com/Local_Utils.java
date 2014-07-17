package com;

import java.io.File;

public class Local_Utils {

	/**
	 * 删除本地文件夹及包含文件
	 * @param dir
	 */
	public static void deleteLocalDir(String dir){
		File file=new File(dir);
		if(file.exists()){
			//delete()方法不能删除非空文件夹，所以得用递归方式将file下所有包含内容删除掉，然后再删除file
			if(file.isDirectory()){
				File[] files=file.listFiles();
				for(File f : files){
					deleteLocalDir(f.getPath());
				}
			}
			file.delete();
		}
	}
}
