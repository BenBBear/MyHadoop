package cn.uc.utils;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public final class CommandHelper {
	public static class CommandResult {
		public static final int EXIT_VALUE_TIMEOUT = 9987;

		private int exitValue;
		private String error;
		private String output;

		public String getError() {
			return error;
		}

		public int getExitValue() {
			return exitValue;
		}

		public String getOutput() {
			return output;
		}

		public void setError(String string) {
			error = string;
		}

		public void setExitValue(int i) {
			exitValue = i;
		}

		public void setOutput(String string) {
			output = string;
		}

	}

	// default time out, in millseconds
	private static int DEFAULT_TIMEOUT = 10 * 1000;
	private static final int DEFAULT_INTERVAL = 1000;

	private static StringBuilder checkoutOutput(BufferedReader errorStreamReader)
			throws IOException {
		StringBuilder buffer = new StringBuilder();
		String line;
		while ((line = errorStreamReader.readLine()) != null) {
			if (buffer.length() > 0) {
				buffer.append("\n");
			}
			buffer.append(line);
		}
		return buffer;
	}

	public static CommandResult exec(String command) throws IOException,
			InterruptedException {
		Process process = Runtime.getRuntime().exec(command);
		CommandResult commandResult = wait(process);
		if (process != null) {
			process.destroy();
		}
		return commandResult;
	}

	public static void main(String[] args) {
		String cmd = "/bin/sh /home/hadoop3u1/hadoop-0.20.2-cdh3u1/bin/hadoop dfs -ls /";
		if (args.length > 0) {
			cmd = args[0];
		}
		System.out.println(cmd);
		try {
			CommandResult result = exec(cmd);
			System.out.println(result.getOutput());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static CommandResult wait(Process process)
			throws InterruptedException, IOException {
		BufferedReader errorStreamReader = null;
		BufferedReader inputStreamReader = null;
		try {
			errorStreamReader = new BufferedReader(new InputStreamReader(
					process.getErrorStream()));
			inputStreamReader = new BufferedReader(new InputStreamReader(
					process.getInputStream()));

			// timeout control
			long startTime = System.currentTimeMillis();
			boolean isFinished = false;

			for (;;) {
				try {
					isFinished = true;
					process.exitValue();
				} catch (IllegalThreadStateException e) {
					isFinished = false;
					Thread.sleep(DEFAULT_INTERVAL);
				}
				if (System.currentTimeMillis() - startTime >= DEFAULT_TIMEOUT) {
					CommandResult result = new CommandResult();
					result.setExitValue(CommandResult.EXIT_VALUE_TIMEOUT);
					result.setOutput("Command process timeout");
					return result;
				}

				if (isFinished) {
					CommandResult result = new CommandResult();
					result.setExitValue(process.waitFor());

					if (errorStreamReader.ready()) {
						StringBuilder buffer = checkoutOutput(errorStreamReader);
						result.setError(buffer.toString());
					}

					if (inputStreamReader.ready()) {
						StringBuilder buffer = checkoutOutput(inputStreamReader);
						result.setOutput(buffer.toString());
					}
					return result;
				}
			}
		} finally {
			if (errorStreamReader != null) {
				try {
					errorStreamReader.close();
				} catch (IOException e) {
				}
			}

			if (inputStreamReader != null) {
				try {
					inputStreamReader.close();
				} catch (IOException e) {
				}
			}
		}
	}

}