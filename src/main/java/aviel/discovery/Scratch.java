package aviel.discovery;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class Scratch {
    private record InstanceHandle(ByteBuffer data) {}

    interface SimpleListener<Value> {
        void onDiscovered(Entry<Value> entity);
        void onDisconnected(InstanceHandle instanceHandle);
    }

    interface EnrichedListener<Value> {
        void onDiscovered(Value entity);
        void onDisconnected(Value entity);
    }

    private record Entry<Value>(InstanceHandle key,
                                Value value) {}

    private enum EntityType {Reader, Writer}

    private record EntityInfo(EntityType type,
                              String partition,
                              String topicName) {}

    interface MetricNameWrapper {}

    interface Reduction<Value1, Value2> {
        <Result> Function<Value2, Result> transform(Function<Value1, Result> problem);

        default <Value3> Reduction<Value1, Value3> compose(Reduction<Value2, Value3> reduction2To3) {
            return new Reduction<>() {
                @Override
                public <Result> Function<Value3, Result> transform(Function<Value1, Result> problem1) {
                    return reduction2To3.transform(Reduction.this.transform(problem1));
                }
            };
        }
    }

    interface ParticipantParams {
        DomainParticipant createDomainParticipant();
    }

    enum Verbosity {
        INFO, DEBUG
    }

    static MetricNameWrapper getDiscoveryMetricNamesWrapper(ParticipantParams participantParams, Verbosity verbosity) {
        return new MetricNameWrapper() {};
    }

    interface DomainParticipant extends NTClosable {
        void openDiscoveryOnWriters(SimpleListener<EntityInfo> listener);
        void openDiscoveryOnReaders(SimpleListener<EntityInfo> listener);
        void close();
    }

    interface Visibility {
        ParticipantParams createParticipantParams();
    }

    interface NTClosable extends AutoCloseable {
        @Override
        void close();
    }

    interface AbstractDiscoverer {
        AbstractDiscoverer withWriters(Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener);
        AbstractDiscoverer withReaders(Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener);
        NTClosable initiate(Visibility visibility);
    }

    static class DiscovererReadersWriters implements AbstractDiscoverer {
        private final Function<MetricNameWrapper, SimpleListener<EntityInfo>> readersListener;
        private final Function<MetricNameWrapper, SimpleListener<EntityInfo>> writersListener;

        public DiscovererReadersWriters(Function<MetricNameWrapper, SimpleListener<EntityInfo>> readersListener,
                                        Function<MetricNameWrapper, SimpleListener<EntityInfo>> writersListener) {
            this.readersListener = readersListener;
            this.writersListener = writersListener;
        }

        @Override
        public AbstractDiscoverer withWriters(Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener) {
            return new DiscovererReadersWriters(readersListener, listenersSequence(writersListener, listener));
        }

        @Override
        public AbstractDiscoverer withReaders(Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener) {
            return new DiscovererReadersWriters(listenersSequence(readersListener, listener), writersListener);
        }

        @Override
        public NTClosable initiate(Visibility visibility) {
            ParticipantParams participantParams = visibility.createParticipantParams();
            DomainParticipant domainParticipant = participantParams.createDomainParticipant();
            MetricNameWrapper metricNamesWrapper = getDiscoveryMetricNamesWrapper(participantParams, Verbosity.INFO);
            domainParticipant.openDiscoveryOnReaders(readersListener.apply(metricNamesWrapper));
            domainParticipant.openDiscoveryOnWriters(writersListener.apply(metricNamesWrapper));
            return domainParticipant;
        }
    }

    static class DiscovererWriters implements AbstractDiscoverer {
        private final Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener;

        public DiscovererWriters(Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener) {
            this.listener = listener;
        }

        @Override
        public AbstractDiscoverer withWriters(Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener) {
            return new DiscovererWriters(listenersSequence(this.listener, listener));
        }

        @Override
        public AbstractDiscoverer withReaders(Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener) {
            return new DiscovererReadersWriters(listener, this.listener);
        }

        @Override
        public NTClosable initiate(Visibility visibility) {
            ParticipantParams participantParams = visibility.createParticipantParams();
            DomainParticipant domainParticipant = participantParams.createDomainParticipant();
            MetricNameWrapper metricNamesWrapper = getDiscoveryMetricNamesWrapper(participantParams, Verbosity.INFO);
            domainParticipant.openDiscoveryOnWriters(listener.apply(metricNamesWrapper));
            return domainParticipant;
        }

    }

    static class DiscovererReaders implements AbstractDiscoverer {
        private final Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener;

        public DiscovererReaders(Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener) {
            this.listener = listener;
        }

        @Override
        public AbstractDiscoverer withWriters(Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener) {
            return new DiscovererReadersWriters(this.listener, listener);
        }

        @Override
        public AbstractDiscoverer withReaders(Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener) {
            return new DiscovererReaders(listenersSequence(this.listener, listener));
        }

        @Override
        public NTClosable initiate(Visibility visibility) {
            ParticipantParams participantParams = visibility.createParticipantParams();
            DomainParticipant domainParticipant = participantParams.createDomainParticipant();
            MetricNameWrapper metricNamesWrapper = getDiscoveryMetricNamesWrapper(participantParams, Verbosity.INFO);
            domainParticipant.openDiscoveryOnReaders(listener.apply(metricNamesWrapper));
            return domainParticipant;
        }
    }

    private static Function<MetricNameWrapper, SimpleListener<EntityInfo>> listenersSequence(Function<MetricNameWrapper, SimpleListener<EntityInfo>> mnwToLsn1,
                                                                                             Function<MetricNameWrapper, SimpleListener<EntityInfo>> mnwToLsn2) {
        return metricNameWrapper -> {
            SimpleListener<EntityInfo> lsn1 = mnwToLsn1.apply(metricNameWrapper);
            SimpleListener<EntityInfo> lsn2 = mnwToLsn2.apply(metricNameWrapper);
            return new SimpleListener<>() {
                @Override
                public void onDiscovered(Entry<EntityInfo> entity) {
                    lsn1.onDiscovered(entity);
                    lsn2.onDiscovered(entity);
                }

                @Override
                public void onDisconnected(InstanceHandle instanceHandle) {
                    lsn1.onDisconnected(instanceHandle);
                    lsn2.onDisconnected(instanceHandle);
                }
            };
        };
    }

    static class DiscovererEmpty implements AbstractDiscoverer {
        @Override
        public AbstractDiscoverer withWriters(Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener) {
            return new DiscovererWriters(listener);
        }

        @Override
        public AbstractDiscoverer withReaders(Function<MetricNameWrapper, SimpleListener<EntityInfo>> listener) {
            return new DiscovererReaders(listener);
        }

        @Override
        public NTClosable initiate(Visibility visibility) {
            return () -> {};
        }
    }

    static Function<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<AbstractDiscoverer, AbstractDiscoverer>> readersDiscovery() {
        return mnwToLsn -> discoverer -> discoverer.withReaders(mnwToLsn);
    }

    static Function<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<AbstractDiscoverer, AbstractDiscoverer>> writersDiscovery() {
        return mnwToLsn -> discoverer -> discoverer.withWriters(mnwToLsn);
    }

    private static <Listener> Reduction<Listener, Listener> identity() {
        return new Reduction<>() {
            @Override
            public <Result> Function<Listener, Result> transform(Function<Listener, Result> problem) {
                return problem;
            }
        };
    }

    static <Entity, MEntity> Reduction<Function<MetricNameWrapper, Consumer<Entity>>, Function<MetricNameWrapper, Consumer<MEntity>>>
    mapDiscoveredOnlyEntity(Function<Entity, MEntity> mapper) {
        return translate(mCns -> mnw -> entity -> mCns.accept(mapper.apply(entity)));
    }

    static <Entity> Reduction<Function<MetricNameWrapper, Consumer<Entity>>, Function<MetricNameWrapper, Consumer<Entity>>>
    discoveredOnlyUnduplicate() {
        return new Reduction<>() {
            @Override
            public <Result> Function<Function<MetricNameWrapper, Consumer<Entity>>, Result> transform(
                    Function<Function<MetricNameWrapper, Consumer<Entity>>, Result> problem) {
                return mnwToCns -> problem.apply(mnw -> {
                    Consumer<Entity> cns = mnwToCns.apply(mnw);
                    Set<Entity> pures = new HashSet<>();
                    return entity -> {
                        if (pures.add(entity)) {
                            cns.accept(entity);
                        }
                    };
                });
            }
        };
    }

    static <Entity> Reduction<Function<MetricNameWrapper, SimpleListener<Entity>>, Function<MetricNameWrapper, EnrichedListener<Entity>>> enrich() {
        return translate(lsn -> mnw -> {
            HashMap<InstanceHandle, Entry<Entity>> entries = new HashMap<>();
            return new SimpleListener<>() {
                @Override
                public void onDiscovered(Entry<Entity> entity) {
                    entries.put(entity.key, entity);
                    lsn.onDiscovered(entity.value);
                }

                @Override
                public void onDisconnected(InstanceHandle instanceHandle) {
                    Entry<Entity> removed = entries.remove(instanceHandle);
                    lsn.onDisconnected(removed.value);
                }
            };
        });
    }

    static <Entity, MEntity> Reduction<Function<MetricNameWrapper, EnrichedListener<Entity>>, Function<MetricNameWrapper, EnrichedListener<MEntity>>>
    mapEnrichedEntity(Function<Entity, MEntity> mapper) {
        return translate(mLsn -> mnw -> new EnrichedListener<>() {
            @Override
            public void onDiscovered(Entity entity) {
                mLsn.onDiscovered(mapper.apply(entity));
            }

            @Override
            public void onDisconnected(Entity entity) {
                mLsn.onDisconnected(mapper.apply(entity));
            }
        });
    }

    static <Entity> Reduction<Function<MetricNameWrapper, EnrichedListener<Entity>>, Function<MetricNameWrapper, EnrichedListener<Entity>>>
    enrichedUnduplicate() {
        return translate(lsn -> mnw -> {
            HashMap<Entity, AtomicInteger> counts = new HashMap<>();
            return new EnrichedListener<Entity>() {
                @Override
                public void onDiscovered(Entity entity) {
                    counts.putIfAbsent(entity, new AtomicInteger(0));
                    if (counts.get(entity).getAndIncrement() == 0) {
                        lsn.onDiscovered(entity);
                    }
                }

                @Override
                public void onDisconnected(Entity entity) {
                    if (counts.get(entity).decrementAndGet() == 0) {
                        lsn.onDisconnected(entity);
                    }
                }
            };
        });
    }

    private static <Entity> Reduction<Function<MetricNameWrapper, SimpleListener<Entity>>, Function<MetricNameWrapper, Consumer<Entity>>> onlyDiscovered() {
        return translate(cns -> mnw -> new SimpleListener<>() {
            @Override
            public void onDiscovered(Entry<Entity> entity) {
                cns.accept(entity.value);
            }

            @Override
            public void onDisconnected(InstanceHandle instanceHandle) {}
        });
    }

    static <ExtendedListener, PureListener> Reduction<Function<MetricNameWrapper, ExtendedListener>, Function<MetricNameWrapper, PureListener>>
    translate(Function<PureListener, Function<MetricNameWrapper, ExtendedListener>> extract) {
        return new Reduction<>() {
            @Override
            public <Result> Function<Function<MetricNameWrapper, PureListener>, Result> transform(
                    Function<Function<MetricNameWrapper, ExtendedListener>, Result> problem) {
                return mnwToLsn -> problem.apply(mnw -> extract.apply(mnwToLsn.apply(mnw)).apply(mnw));
            }
        };
    }

    static <Listener> Function<AbstractDiscoverer, AbstractDiscoverer>
    applyDiscovery(Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Listener>> reduction,
                   Function<MetricNameWrapper, Listener> listener,
                   Function<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<AbstractDiscoverer, AbstractDiscoverer>> discoveryInitiator) {
        return reduction.transform(discoveryInitiator).apply(listener);
    }

    public static void main(String[] args) {
        Random random = new Random();

        Visibility someVisibility = () -> () -> new DomainParticipant() {
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
        try (NTClosable discoveryCloser =
                     new FriendlyDiscovery()
                             .enrich()
                             .writers(metricNameWrapper -> new EnrichedListener<>() {
                                 @Override
                                 public void onDiscovered(EntityInfo entity) {
                                     System.out.println("+W " + entity);
                                 }

                                 @Override
                                 public void onDisconnected(EntityInfo entity) {
                                     System.out.println("-W " + entity);
                                 }
                             })
                             .enrich()
                             .readers(metricNameWrapper -> new EnrichedListener<>() {
                                 @Override
                                 public void onDiscovered(EntityInfo entity) {
                                     System.out.println("+R " + entity);
                                 }

                                 @Override
                                 public void onDisconnected(EntityInfo entity) {
                                     System.out.println("-R " + entity);
                                 }
                             })
                             .enrich()
                             .map(EntityInfo::topicName)
                             .unduplicate()
                             .writers(metricNameWrapper -> new EnrichedListener<>() {
                                 @Override
                                 public void onDiscovered(String entity) {
                                     System.out.println("+WT " + entity);
                                 }

                                 @Override
                                 public void onDisconnected(String entity) {
                                     System.out.println("-WT " + entity);
                                 }
                             })
                             .discoveredOnly()
                             .map(EntityInfo::partition)
                             .unduplicate()
                             .readers(metricNameWrapper -> partition -> System.out.println("+RP " + partition))
                             .initiate(someVisibility)) {
            int count = 50;
            System.out.printf("[%s]%n", " ".repeat(count));
            for (int i = 0; i < count; i++) {
                Thread.sleep(500);
                System.out.printf("[%s%s]%n", "x".repeat(i + 1), " ".repeat(count - i - 1));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static class FriendlyDiscovery extends FriendlySimpleDiscovery<EntityInfo> {
        public FriendlyDiscovery() {
            this(Function.identity());
        }

        FriendlyDiscovery(Function<AbstractDiscoverer, AbstractDiscoverer> discovery) {
            super(discovery, identity());
        }

        NTClosable initiate(Visibility visibility) {
            return discovery.apply(new DiscovererEmpty()).initiate(visibility);
        }
    }

    static class FriendlySimpleDiscovery<Entity> {
        final Function<AbstractDiscoverer, AbstractDiscoverer> discovery;
        final Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, SimpleListener<Entity>>> reduction;

        FriendlySimpleDiscovery(Function<AbstractDiscoverer, AbstractDiscoverer> discovery,
                                Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, SimpleListener<Entity>>> reduction) {
            this.discovery = discovery;
            this.reduction = reduction;
        }

        FriendlyDiscovery readers(Function<MetricNameWrapper, SimpleListener<Entity>> mnwToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.transform(Scratch.readersDiscovery()).apply(mnwToLsn)));
        }

        FriendlyDiscovery writers(Function<MetricNameWrapper, SimpleListener<Entity>> mnwToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.transform(Scratch.writersDiscovery()).apply(mnwToLsn)));
        }

        <MEntity> FriendlySimpleDiscovery<MEntity> map(Function<Entity, MEntity> mapper) {
            return new FriendlySimpleDiscovery<>(discovery, reduction.compose(translate(mLsn -> mnw -> new SimpleListener<>() {
                @Override
                public void onDiscovered(Entry<Entity> entity) {
                    mLsn.onDiscovered(new Entry<>(entity.key, mapper.apply(entity.value)));
                }

                @Override
                public void onDisconnected(InstanceHandle instanceHandle) {
                    mLsn.onDisconnected(instanceHandle);
                }
            })));
        }

        FriendlyEnrichedDiscovery<Entity> enrich() {
            return new FriendlyEnrichedDiscovery<>(discovery, reduction.compose(Scratch.enrich()));
        }

        FriendlyDiscoveredOnlyDiscovery<Entity> discoveredOnly() {
            return new FriendlyDiscoveredOnlyDiscovery<>(discovery, reduction.compose(onlyDiscovered()));
        }
    }

    static class FriendlyDiscoveredOnlyDiscovery<Entity> {
        final Function<AbstractDiscoverer, AbstractDiscoverer> discovery;
        final Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Consumer<Entity>>> reduction;

        FriendlyDiscoveredOnlyDiscovery(Function<AbstractDiscoverer, AbstractDiscoverer> discovery,
                                        Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Consumer<Entity>>> reduction) {
            this.discovery = discovery;
            this.reduction = reduction;
        }

        FriendlyDiscovery readers(Function<MetricNameWrapper, Consumer<Entity>> mnwToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.transform(Scratch.readersDiscovery()).apply(mnwToLsn)));
        }

        FriendlyDiscovery writers(Function<MetricNameWrapper, Consumer<Entity>> mnwToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.transform(Scratch.writersDiscovery()).apply(mnwToLsn)));
        }

        <MEntity> FriendlyDiscoveredOnlyDiscovery<MEntity> map(Function<Entity, MEntity> mapper) {
            return new FriendlyDiscoveredOnlyDiscovery<>(discovery, reduction.compose(mapDiscoveredOnlyEntity(mapper)));
        }

        FriendlyDiscoveredOnlyDiscovery<Entity> unduplicate() {
            return new FriendlyDiscoveredOnlyDiscovery<>(discovery, reduction.compose(discoveredOnlyUnduplicate()));
        }
    }

    static class FriendlyEnrichedDiscovery<Entity> {
        final Function<AbstractDiscoverer, AbstractDiscoverer> discovery;
        final Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, EnrichedListener<Entity>>> reduction;

        FriendlyEnrichedDiscovery(Function<AbstractDiscoverer, AbstractDiscoverer> discovery,
                                  Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, EnrichedListener<Entity>>> reduction) {
            this.discovery = discovery;
            this.reduction = reduction;
        }

        FriendlyDiscovery readers(Function<MetricNameWrapper, EnrichedListener<Entity>> mnwToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.transform(Scratch.readersDiscovery()).apply(mnwToLsn)));
        }

        FriendlyDiscovery writers(Function<MetricNameWrapper, EnrichedListener<Entity>> mnwToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.transform(Scratch.writersDiscovery()).apply(mnwToLsn)));
        }

        <MEntity> FriendlyEnrichedDiscovery<MEntity> map(Function<Entity, MEntity> mapper) {
            return new FriendlyEnrichedDiscovery<>(discovery, reduction.compose(mapEnrichedEntity(mapper)));
        }

        FriendlyEnrichedDiscovery<Entity> unduplicate() {
            return new FriendlyEnrichedDiscovery<>(discovery, reduction.compose(enrichedUnduplicate()));
        }
    }
}
