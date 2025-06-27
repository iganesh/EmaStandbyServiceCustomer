package com.example.demo;
package com.example.emasessionmanager;
//zookeeper.connect.string=localhost:2181 zookeeper.latch.path=/ema/leader
/*
<dependency>
            <groupId>org.apache.curator</groupId>
            <artifactId>curator-framework</artifactId>
            <version>${curator.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.curator</groupId>
            <artifactId>curator-recipes</artifactId>
            <version>${curator.version}</version>
        </dependency>
 */
import com.refinitiv.ema.access.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
public class SessionManagerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SessionManagerApplication.class, args);
    }

    @Configuration
    public static class EmaConfig {

        @Value("${ema.active.host:localhost}")
        private String activeHost;

        @Value("${ema.active.port:14002}")
        private String activePort;

        @Value("${ema.standby.host:localhost}")
        private String standbyHost;

        @Value("${ema.standby.port:14003}")
        private String standbyPort;

        @Value("${ema.service:ELEKTRON_DD}")
        private String serviceName;

        @Value("${ema.item:IBM.N}")
        private String itemName;

        @Value("${zookeeper.connect.string:localhost:2181}")
        private String zookeeperConnectString;

        @Value("${zookeeper.latch.path:/ema/leader}")
        private String latchPath;

        @Bean
        public OmmConsumerConfig ommConsumerConfig() {
            OmmConsumerConfig config = OmmConsumerConfig.create();
            config.addChannel("Channel_Active", Map.create()
                    .add(MapEntry.create("ChannelType", OmmConsumerConfig.ChannelTypeEnum.RSSL_SOCKET.getValue())
                            .add(MapEntry.create("Host", activeHost))
                            .add(MapEntry.create("Port", activePort))));
            config.addChannel("Channel_Standby", Map.create()
                    .add(MapEntry.create("ChannelType", OmmConsumerConfig.ChannelTypeEnum.RSSL_SOCKET.getValue())
                            .add(MapEntry.create("Host", standbyHost))
                            .add(MapEntry.create("Port", standbyPort))));
            config.addChannelSet("ChannelSet_1", new String[]{"Channel_Active", "Channel_Standby"});
            config.operationModel(OmmConsumerConfig.OperationModel.USER_DISPATCH);
            return config;
        }

        @Bean
        public CuratorFramework curatorFramework() {
            CuratorFramework client = CuratorFrameworkFactory.newClient(
                    zookeeperConnectString,
                    new ExponentialBackoffRetry(1000, 3));
            client.start();
            return client;
        }

        @Bean
        public LeaderLatch leaderLatch(CuratorFramework curatorFramework) {
            String hostId = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "host-" + System.currentTimeMillis();
            return new LeaderLatch(curatorFramework, latchPath, hostId);
        }

        @Bean
        public ZooKeeperCoordinator zooKeeperCoordinator(LeaderLatch leaderLatch) {
            return new ZooKeeperCoordinator(leaderLatch);
        }

        @Bean
        public SessionManager sessionManager(OmmConsumerConfig config, ZooKeeperCoordinator coordinator) {
            return new SessionManager(config, serviceName, itemName, coordinator);
        }
    }

    public static class ZooKeeperCoordinator {

        private final LeaderLatch leaderLatch;
        private final AtomicBoolean isLeader = new AtomicBoolean(false);
        private final String hostId = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "host-" + System.currentTimeMillis();

        public ZooKeeperCoordinator(LeaderLatch leaderLatch) {
            this.leaderLatch = leaderLatch;
        }

        public void start() throws Exception {
            leaderLatch.start();
            leaderLatch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    isLeader.set(true);
                    System.out.println("Host " + hostId + " is leader");
                }

                @Override
                public void notLeader() {
                    isLeader.set(false);
                    System.out.println("Host " + hostId + " is not leader");
                }
            });
        }

        public boolean isLeader() {
            return isLeader.get();
        }

        public void stop() throws Exception {
            leaderLatch.close();
        }
    }

    public static class SessionManager implements OmmConsumerClient {

        private final OmmConsumerConfig config;
        private final String serviceName;
        private final String itemName;
        private final ZooKeeperCoordinator coordinator;
        private OmmConsumer consumer;
        private final AtomicBoolean isActive = new AtomicBoolean(false);
        private long itemHandle;
        private final String hostId = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "host-" + System.currentTimeMillis();
        private static final long DISPATCH_INTERVAL = 1000; // 1 second

        public SessionManager(OmmConsumerConfig config, String serviceName, String itemName, ZooKeeperCoordinator coordinator) {
            this.config = config;
            this.serviceName = serviceName;
            this.itemName = itemName;
            this.coordinator = coordinator;
        }

        @PostConstruct
        public void init() throws Exception {
            coordinator.start();
            new Thread(this::coordinateSession).start();
        }

        private void coordinateSession() {
            while (!Thread.interrupted()) {
                try {
                    if (coordinator.isLeader() && !isActive.get()) {
                        startConsumer();
                    } else if (!coordinator.isLeader() && isActive.get()) {
                        stopConsumer();
                    }

                    if (isActive.get() && consumer != null) {
                        consumer.dispatch(DISPATCH_INTERVAL);
                    }

                    Thread.sleep(DISPATCH_INTERVAL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("Error in coordination for host " + hostId + ": " + e.getMessage());
                }
            }
        }

        private void startConsumer() {
            try {
                consumer = OmmConsumer.create(config.consumerName("Consumer_" + hostId)
                        .channelSet("ChannelSet_1"), this);
                itemHandle = consumer.registerClient(ReqMsg.create()
                        .serviceName(serviceName)
                        .name(itemName), this);
                isActive.set(true);
                System.out.println("Host " + hostId + " is active consumer on ChannelSet_1 (Active: " + config.getConfig().get("Channel_Active", "Host") + ":" + config.getConfig().get("Channel_Active", "Port") + ", Standby: " + config.getConfig().get("Channel_Standby", "Host") + ":" + config.getConfig().get("Channel_Standby", "Port") + ")");
            } catch (OmmException e) {
                System.err.println("Failed to start consumer for host " + hostId + ": " + e.getMessage());
                isActive.set(false);
            }
        }

        private void stopConsumer() {
            if (consumer != null) {
                consumer.uninitialize();
                consumer = null;
                isActive.set(false);
                System.out.println("Host " + hostId + " stopped consumer");
            }
        }

        @Override
        public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event) {
            System.out.println("Host " + hostId + " received RefreshMsg: " + refreshMsg);
            if (refreshMsg.hasState() && refreshMsg.state().streamState() == OmmState.StreamState.OPEN &&
                    refreshMsg.state().dataState() == OmmState.DataState.OK) {
                System.out.println("Host " + hostId + " connection is up");
            }
        }

        @Override
        public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent event) {
            System.out.println("Host " + hostId + " received UpdateMsg: " + updateMsg);
        }

        @Override
        public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent event) {
            System.out.println("Host " + hostId + " received StatusMsg: " + statusMsg);
            if (statusMsg.hasState() && statusMsg.state().streamState() != OmmState.StreamState.OPEN) {
                System.err.println("Host " + hostId + " connection down: " + statusMsg.state());
                stopConsumer();
            }
        }

        @PreDestroy
        public void shutdown() {
            stopConsumer();
            try {
                coordinator.stop();
            } catch (Exception e) {
                System.err.println("Error shutting down ZooKeeper coordinator: " + e.getMessage());
            }
            System.out.println("Host " + hostId + " shut down");
        }
    }
}