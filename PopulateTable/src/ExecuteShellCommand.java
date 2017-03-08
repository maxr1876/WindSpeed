import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;


public class ExecuteShellCommand {
	
	
	public String executeCommand(String command) {

		StringBuffer output = new StringBuffer();
		Process p;
		try {
			String [] args = command.split(" ");
			String cmd = args[0];
			ProcessBuilder pb = new ProcessBuilder().command("nice", "-19", cmd, args[1], args[2], args[3], args[4], args[5]).directory(new File("../../../../../../../../../../../../../s/bach/j/under/mroseliu/MethaneMapping/grib2/wgrib2"));
			p = pb.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			p.waitFor();
			String line = "";
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			return e.toString();
		}
		return output.toString();

	}
	
}
