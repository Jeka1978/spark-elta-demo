package multithreading_demo;

import lombok.SneakyThrows;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Evgeny Borisov
 */
public class Main {
    @SneakyThrows
    public static void main(String[] args) {

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Future<String> future = executorService.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return "mama";
            }
        });




        OnePrinter onePrinter = new OnePrinter();
        Thread t1 = new Thread(onePrinter);
        t1.start();

        boolean moreWorkTodo=true;
        new Thread(() -> {
            while (moreWorkTodo){
                System.out.println("2");
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        Thread.sleep(1000);
        onePrinter.setMoreWorkToDo(false);
    }
}
