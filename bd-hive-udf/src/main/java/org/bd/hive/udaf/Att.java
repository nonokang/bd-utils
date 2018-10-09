package org.bd.hive.udaf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Att {

	private static final String ROOT_PATH = System.getProperty("user.dir");

	public static void main(String[] args) throws IOException {

		System.out.println(ROOT_PATH);
		String command = runtimeCmd()+"#cd "+ ROOT_PATH + " && mvn clean package";
		System.out.println(command);
		String[] commands = command.split("#");
		execCmd(commands);
		
	}

	public static String runtimeCmd() {
		String os = System.getProperty("os.name").toLowerCase();
		if(os.indexOf("windows")>=0) {
			return "cmd#/c" ;
		}else if(os.indexOf("linux")>=0) {
			return "/bin/sh#-c";
		}
		return null;
	}

	public static int execCmd(String[] commands) {
		BufferedReader br = null;
		Process process = null;
		try {
			process = Runtime.getRuntime().exec(commands);
			br = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line = null;
			while ((line = br.readLine()) != null || "".equals(line = br.readLine())) {
				System.out.println(line);
			}
			BufferedReader errors = new BufferedReader(new InputStreamReader(process.getErrorStream()));
			String line2 = null;
			while ((line2 = errors.readLine()) != null || "".equals(line2 = errors.readLine())) {
				System.out.print(line2);
			}
			return process.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.print(e.getMessage());
		} finally {
			process.destroy();
			if (br != null) {
				try {
					br.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return -1;
	}
}
