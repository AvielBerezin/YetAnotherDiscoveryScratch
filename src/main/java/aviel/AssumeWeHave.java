package aviel;

import aviel.discovery.DiscoveryBasics.NTClosable;
import aviel.discovery.Listeners.SimpleListener;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AssumeWeHave {
    public static DomainParticipant createDomainParticipant() {
        Random random = new Random();
        return new DomainParticipant() {
            boolean closed = false;
            final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setDaemon(true);
                return thread;
            });

            private void startDiscovery(EntityType entityType, SimpleListener<EntityInfo> listener) {
                scheduler.schedule(() -> {
                    if (closed) return;
                    byte[] array = new byte[4];
                    random.nextBytes(array);
                    InstanceHandle key = new InstanceHandle(ByteBuffer.wrap(array));
                    listener.onDiscovered(new Entry<>(key,
                                                      new EntityInfo(entityType,
                                                                     List.of("hi", "bye", "die").get(random.nextInt(3)),
                                                                     List.of("Shuly", "rooly", "gooly").get(random.nextInt(3)))));
                    scheduler.schedule(() -> {
                        if (closed) return;
                        listener.onDisconnected(key);
                    }, random.nextInt(5000), TimeUnit.MILLISECONDS);
                    startDiscovery(entityType, listener);
                }, random.nextInt(1000), TimeUnit.MILLISECONDS);
            }

            @Override
            public void openDiscoveryOnWriters(SimpleListener<EntityInfo> listener) {
                startDiscovery(EntityType.Writer, listener);
            }

            @Override
            public void openDiscoveryOnReaders(SimpleListener<EntityInfo> listener) {
                startDiscovery(EntityType.Reader, listener);
            }

            @Override
            public void close() {
                closed = true;
            }
        };
    }

    public static MetricReporter createMetricReporter(MetricNameWrapper metricNameWrapper, Verbosity verbosity) {
        return new MetricReporter() {};
    }

    public static MetricNameWrapper getDiscoveryMetricNamesWrapper(ParticipantParams participantParams) {
        return new MetricNameWrapper() {};
    }

    public enum EntityType {Reader, Writer}

    public enum Verbosity {
        INFO, DEBUG
    }

    public interface MetricNameWrapper {}

    public interface ParticipantParams {
        DomainParticipant createDomainParticipant();
    }

    public interface MetricReporter {}

    public interface DomainParticipant extends NTClosable {
        void openDiscoveryOnWriters(SimpleListener<EntityInfo> listener);
        void openDiscoveryOnReaders(SimpleListener<EntityInfo> listener);
        void close();
    }

    public record InstanceHandle(ByteBuffer data) {}

    public record Entry<Value>(InstanceHandle key,
                               Value value) {}

    public record EntityInfo(EntityType type,
                             String partition,
                             String topicName) {}

    public static void loadingBar(int length, Duration duration) throws InterruptedException {
        Duration step = duration.dividedBy(length);
        System.out.printf("[%s]%n", " ".repeat(length));
        for (int i = 0; i < length; i++) {
            Thread.sleep(step.toMillis());
            System.out.printf("[%s%s]%n", "x".repeat(i + 1), " ".repeat(length - i - 1));
        }
    }
}
