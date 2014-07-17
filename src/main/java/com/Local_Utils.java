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
