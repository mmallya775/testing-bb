// Forever.java
public class Forever {
  public static void main(String[] args) throws InterruptedException {
    Runtime.getRuntime()
        .addShutdownHook(new Thread(() -> System.out.println("[java] Shutdown signal received, cleaning up...")));

    long i = 0;
    while (true) {
      System.out.println("[java] tick " + i++);
      Thread.sleep(1000);
    }
  }
}
