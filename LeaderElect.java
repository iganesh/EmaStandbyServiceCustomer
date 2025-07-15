ava -Dhost.id=host1 -cp .:curator-dependencies.jar LeaderElectionExample
java -Dhost.id=host2 -cp .:curator-dependencies.jar LeaderElectionExample

    
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

public class LeaderElectionExample {
    private static final String ZK_CONNECTION_STRING = "localhost:2181"; // Replace with your ZooKeeper ensemble
    private static final String LEADER_PATH = "/leader/election";
    private static final String HOST_ID = System.getProperty("host.id", "host-" + System.currentTimeMillis());

    public static void main(String[] args) throws Exception {
        // Create Curator client with retry policy
        CuratorFramework client = CuratorFrameworkFactory.newClient(
            ZK_CONNECTION_STRING,
            new ExponentialBackoffRetry(1000, 3)
        );
        client.start();

        // Create leader selector
        LeaderSelector leaderSelector = new LeaderSelector(client, LEADER_PATH, new LeaderSelectorListenerAdapter() {
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                // This method is called when the instance becomes the leader
                System.out.println(HOST_ID + " is now the leader!");
                try {
                    // Simulate doing work as the leader (hold leadership indefinitely or until interrupted)
                    while (true) {
                        System.out.println(HOST_ID + " is performing leader tasks...");
                        Thread.sleep(5000); // Simulate work every 5 seconds
                    }
                } catch (InterruptedException e) {
                    System.out.println(HOST_ID + " leadership interrupted.");
                    Thread.currentThread().interrupt();
                }
            }
        });

        // Auto-requeue to participate in leader election again if leadership is lost
        leaderSelector.autoRequeue();
        leaderSelector.start();

        System.out.println(HOST_ID + " started and participating in leader election.");

        // Keep the application running
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            System.out.println(HOST_ID + " shutting down.");
            Thread.currentThread().interrupt();
        } finally {
            leaderSelector.close();
            client.close();
        }
    }
}
