import java.util.Timer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author changbp
 * @Date 2021-06-18 11:34
 * @Return
 * @Version 1.0
 */
public class ThreadDemo {
    public static void main(String[] args) {
        final AtomicInteger atomicInteger = new AtomicInteger();

        //动态线程池，
        final ExecutorService executorService = Executors.newCachedThreadPool();
        //单线程池
        final ExecutorService executorService1 = Executors.newSingleThreadExecutor();
        //可定时走起执行的单线程池
        final ExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        //定长线程池
        final ExecutorService executorService2 = Executors.newFixedThreadPool(5);
        //可定时周期执行的定长线程池
        final ScheduledExecutorService scheduledExecutorService1 = Executors.newScheduledThreadPool(5);
        //可拆分任务执行的线程池
        final ForkJoinPool forkJoinPool = new ForkJoinPool();

        Timer timer = new Timer();
        
    }
}
