package multithreading_demo;

import lombok.Setter;

/**
 * @author Evgeny Borisov
 */
public class OnePrinter implements Runnable{
    @Setter
    private boolean moreWorkToDo=true;

    @Override
    public void run() {
        while (moreWorkToDo) {
            System.out.println("1111");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                moreWorkToDo = false;
            }
        }
    }
}
