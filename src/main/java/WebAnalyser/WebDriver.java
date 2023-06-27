package WebAnalyser;


import java.util.Objects;

public class WebDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: WebAnalyser <jobType> <input path> <output path>");
            System.exit(-1);
        }
        if(Objects.equals(args[0], "All") || Objects.equals(args[0], "RequestCnt")){
            String [] job1args = new String[2];
            job1args[0] = args[1];
            job1args[1] = args[2] + "/01RequestCnt";
            int job1stat = RequestCnt.main(job1args);
            if (job1stat == 0) {
                System.out.println("Job 1 [RequestCnt] finished successfully");
            } else {
                System.out.println("Job 1 [RequestCnt] failed");
            }
        }
        if (Objects.equals(args[0], "All") || Objects.equals(args[0], "AddressCnt")) {
            String [] job2args = new String[2];
            job2args[0] = args[1];
            job2args[1] = args[2] + "/02AddressCnt";
            int job2stat = AddressCnt.main(job2args);
            if (job2stat == 0) {
                System.out.println("Job 2 [AddressCnt] finished successfully");
            } else {
                System.out.println("Job 2 [AddressCnt] failed");
            }
        }
        if (Objects.equals(args[0], "All") || Objects.equals(args[0], "AccessInHour")) {
            String [] job3args = new String[2];
            job3args[0] = args[1];
            job3args[1] = args[2] + "/03AccessInHour";
            int job3stat = AccessInHour.main(job3args);
            if (job3stat == 0) {
                System.out.println("Job 3 [AccessInHour] finished successfully");
            } else {
                System.out.println("Job 3 [AccessInHour] failed");
            }
        }
        if (Objects.equals(args[0], "All") || Objects.equals(args[0], "AccessInHourSum")) {
            String [] job4args = new String[2];
            job4args[0] = args[1];
            job4args[1] = args[2] + "/04AccessInHourSum";
            int job4stat = AccessInHourSum.main(job4args);
            if (job4stat == 0) {
                System.out.println("Job 4 [AccessInHourSum] finished successfully");
            } else {
                System.out.println("Job 4 [AccessInHourSum] failed");
            }
        }

        if (Objects.equals(args[0], "All") || Objects.equals(args[0], "AgentType")) {
            String [] job5args = new String[2];
            job5args[0] = args[1];
            job5args[1] = args[2] + "/05AgentType";
            int job5stat = AgentType.main(job5args);
            if (job5stat == 0) {
                System.out.println("Job 5 [AgentType] finished successfully");
            } else {
                System.out.println("Job 5 [AgentType] failed");
            }
        }

        if(Objects.equals(args[0], "All") || Objects.equals(args[0], "DeviceType")) {
            String [] job6args = new String[2];
            job6args[0] = args[1];
            job6args[1] = args[2] + "/06DeviceType";
            int job6stat = DeviceType.main(job6args);
            if (job6stat == 0) {
                System.out.println("Job 6 [DeviceType] finished successfully");
            } else {
                System.out.println("Job 6 [DeviceType] failed");
            }
        }
    }
}
