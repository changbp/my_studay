import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author changbp
 * @Date 2021-06-16 18:09
 * @Return
 * @Version 1.0
 */
public class Demo {
    public static void main(String[] args) {
        List<Job> jobs = new ArrayList<>();
        jobs.add(new Job(1, "2022-09-01 01:00:00"));
        jobs.add(new Job(2, "2022-09-01 00:00:00"));
        jobs.add(new Job(1, "2022-09-01 01:00:00"));
        jobs.add(new Job(3, "2022-09-01 02:00:00"));
        jobs.add(new Job(1, "2022-09-01 01:00:00"));
        jobs.add(new Job(2, "2022-09-01 10:00:00"));
        printRes(jobs);
    }

    /**
     * 统计每个任务在0-23小时每小时运行的任务次数(注意存在一些没有运行任务的时间，请补0)
     * 任务1,0点0次，1点0次，2点0次，3点1次...，23 0次
     */
    public static void printRes(List<Job> instanceList) {
        Stream<Job> stream = instanceList.stream();
        Map<Integer, List<Job>> collect = stream.collect(Collectors.groupingBy(Job::getId));

        collect.forEach((k, v) -> {
            StringBuffer stringBuffer = new StringBuffer();
            try {
                for (Job j : v) {
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    SimpleDateFormat format1 = new SimpleDateFormat("HH");
                    Date date = format.parse(j.getTime());
                    String hour = format1.format(date);
                    stringBuffer.append("，").append(hour).append("点");
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
            System.out.println("任务 " + k  + stringBuffer.toString());
        });

    }

}

class Job {
    private Integer id;
    private String time;

    public Job(Integer id, String time) {
        this.id = id;
        this.time = time;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}