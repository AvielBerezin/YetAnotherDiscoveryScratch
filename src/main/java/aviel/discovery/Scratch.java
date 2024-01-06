package aviel.discovery;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

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

    static MetricNameWrapper getDiscoveryMetricNamesWrapper(ParticipantParams participantParams) {
        return new MetricNameWrapper() {};
    }

    interface MetricReporter {}

    static MetricReporter createMetricReporter(MetricNameWrapper metricNameWrapper, Verbosity verbosity) {
        return new MetricReporter() {};
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
            MetricNameWrapper metricNamesWrapper = getDiscoveryMetricNamesWrapper(participantParams);
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
            MetricNameWrapper metricNamesWrapper = getDiscoveryMetricNamesWrapper(participantParams);
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
            MetricNameWrapper metricNamesWrapper = getDiscoveryMetricNamesWrapper(participantParams);
            domainParticipant.openDiscoveryOnReaders(listener.apply(metricNamesWrapper));
            return domainParticipant;
        }
    }

    private static <From> Function<From, SimpleListener<EntityInfo>> listenersSequence(Function<From, SimpleListener<EntityInfo>> mrToLsn1,
                                                                                       Function<From, SimpleListener<EntityInfo>> mrToLsn2) {
        return metricNameWrapper -> {
            SimpleListener<EntityInfo> lsn1 = mrToLsn1.apply(metricNameWrapper);
            SimpleListener<EntityInfo> lsn2 = mrToLsn2.apply(metricNameWrapper);
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

    static <Listener> Reduction<Function<MetricNameWrapper, Listener>, Function<MetricNameWrapper, Function<MetricReporter, Listener>>>
    useMetricReporter(Verbosity verbosity) {
        return new Reduction<>() {
            @Override
            public <Result> Function<Function<MetricNameWrapper, Function<MetricReporter, Listener>>, Result> transform(Function<Function<MetricNameWrapper, Listener>, Result> problem) {
                return vMnwToLsn -> problem.apply(mnw -> vMnwToLsn.apply(mnw)
                                                                  .apply(createMetricReporter(mnw, verbosity)));
            }
        };
    }

    static <Listener> Reduction<Function<MetricNameWrapper, Function<MetricReporter, Listener>>, Function<MetricNameWrapper, Listener>>
    omitMetricReporter() {
        return new Reduction<>() {
            @Override
            public <Result> Function<Function<MetricNameWrapper, Listener>, Result> transform(Function<Function<MetricNameWrapper, Function<MetricReporter, Listener>>, Result> problem) {
                return mnwToLsn -> problem.apply(mnw -> mr -> mnwToLsn.apply(mnw));
            }
        };
    }

    private static <Entity> Reduction<Function<MetricReporter, SimpleListener<Entity>>, Function<MetricReporter, Consumer<Entity>>>
    onlyDiscovered() {
        return translate(cns -> mr -> new SimpleListener<>() {
            @Override
            public void onDiscovered(Entry<Entity> entity) {
                cns.accept(entity.value);
            }

            @Override
            public void onDisconnected(InstanceHandle instanceHandle) {}
        });
    }

    static <Entity, MEntity> Reduction<Function<MetricReporter, Consumer<Entity>>, Function<MetricReporter, Consumer<MEntity>>>
    mapDiscoveredOnlyEntity(Function<Entity, MEntity> mapper) {
        return translate(mCns -> mr -> entity -> mCns.accept(mapper.apply(entity)));
    }

    static <Entity> Reduction<Function<MetricReporter, Consumer<Entity>>, Function<MetricReporter, Consumer<Entity>>>
    discoveredOnlyUnduplicate() {
        return new Reduction<>() {
            @Override
            public <Result> Function<Function<MetricReporter, Consumer<Entity>>, Result> transform(
                    Function<Function<MetricReporter, Consumer<Entity>>, Result> problem) {
                return mnwToCns -> problem.apply(mr -> {
                    Consumer<Entity> cns = mnwToCns.apply(mr);
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

    static <Entity> Reduction<Function<MetricReporter, SimpleListener<Entity>>, Function<MetricReporter, EnrichedListener<Entity>>>
    enrich() {
        return translate(lsn -> mr -> {
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

    static <Entity, MEntity> Reduction<Function<MetricReporter, EnrichedListener<Entity>>, Function<MetricReporter, EnrichedListener<MEntity>>>
    mapEnrichedEntity(Function<Entity, MEntity> mapper) {
        return translate(mLsn -> mr -> new EnrichedListener<>() {
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

    static <Entity> Reduction<Function<MetricReporter, EnrichedListener<Entity>>, Function<MetricReporter, EnrichedListener<Entity>>>
    enrichedUnduplicate() {
        return translate(lsn -> mr -> {
            HashMap<Entity, AtomicInteger> counts = new HashMap<>();
            return new EnrichedListener<>() {
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

    private static <Entity, MEntity> Reduction<Function<MetricReporter, SimpleListener<Entity>>, Function<MetricReporter, SimpleListener<MEntity>>>
    mapSimpleListener(Function<Entity, MEntity> mapper) {
        return Scratch.translate(mLsn -> mr -> new SimpleListener<>() {
            @Override
            public void onDiscovered(Entry<Entity> entity) {
                mLsn.onDiscovered(new Entry<>(entity.key, mapper.apply(entity.value)));
            }

            @Override
            public void onDisconnected(InstanceHandle instanceHandle) {
                mLsn.onDisconnected(instanceHandle);
            }
        });
    }

    private static <Entity> Reduction<Function<MetricReporter, SimpleListener<Entity>>, Function<MetricReporter, SimpleListener<Entity>>>
    filterSimpleListener(Predicate<Entity> predicate) {
        return Scratch.translate(lsn -> mr -> {
            Set<InstanceHandle> filteredIn = Collections.synchronizedSet(new HashSet<>());
            return new SimpleListener<>() {
                @Override
                public void onDiscovered(Entry<Entity> entity) {
                    if (predicate.test(entity.value)) {
                        filteredIn.add(entity.key);
                        lsn.onDiscovered(entity);
                    }
                }

                @Override
                public void onDisconnected(InstanceHandle instanceHandle) {
                    if (filteredIn.remove(instanceHandle)) {
                        lsn.onDisconnected(instanceHandle);
                    }
                }
            };
        });
    }

    static <Param, Lsn1, Lsn2> Reduction<Function<Param, Lsn1>, Function<Param, Lsn2>> ignoreParameter(Reduction<Lsn1, Lsn2> reduction) {
        return new Reduction<>() {
            @Override
            public <Result> Function<Function<Param, Lsn2>, Result> transform(Function<Function<Param, Lsn1>, Result> problem) {
                return pToLsn -> problem.apply(param -> reduction.transform(lsn1 -> lsn1)
                                                                 .apply(pToLsn.apply(param)));
            }
        };
    }

    static <ExtendedListener, PureListener> Reduction<Function<MetricReporter, ExtendedListener>, Function<MetricReporter, PureListener>>
    translate(Function<PureListener, Function<MetricReporter, ExtendedListener>> extract) {
        return new Reduction<>() {
            @Override
            public <Result> Function<Function<MetricReporter, PureListener>, Result> transform(
                    Function<Function<MetricReporter, ExtendedListener>, Result> problem) {
                return vMnwToLsn -> problem.apply(mr -> extract.apply(vMnwToLsn.apply(mr)).apply(mr));
            }
        };
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
            super(discovery, Scratch.useMetricReporter(Verbosity.DEBUG));
        }

        NTClosable initiate(Visibility visibility) {
            return discovery.apply(new DiscovererEmpty()).initiate(visibility);
        }
    }

    static class FriendlySimpleDiscovery<Entity> {
        final Function<AbstractDiscoverer, AbstractDiscoverer> discovery;
        final Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<MetricReporter, SimpleListener<Entity>>>> reduction;

        FriendlySimpleDiscovery(Function<AbstractDiscoverer, AbstractDiscoverer> discovery,
                                Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<MetricReporter, SimpleListener<Entity>>>> reduction) {
            this.discovery = discovery;
            this.reduction = reduction;
        }

        FriendlyDiscovery readers(Function<MetricReporter, SimpleListener<Entity>> mrToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.compose(omitMetricReporter())
                                                                    .compose(useMetricReporter(Verbosity.INFO))
                                                                    .transform(Scratch.readersDiscovery())
                                                                    .apply(mnw -> mrToLsn)));
        }

        FriendlyDiscovery writers(Function<MetricReporter, SimpleListener<Entity>> mrToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.compose(omitMetricReporter())
                                                                    .compose(useMetricReporter(Verbosity.INFO))
                                                                    .transform(Scratch.writersDiscovery())
                                                                    .apply(mnw -> mrToLsn)));
        }

        <MEntity> FriendlySimpleDiscovery<MEntity> map(Function<Entity, MEntity> mapper) {
            return new FriendlySimpleDiscovery<>(discovery, reduction.compose(ignoreParameter(mapSimpleListener(mapper))));
        }

        FriendlySimpleDiscovery<Entity> filter(Predicate<Entity> predicate) {
            return new FriendlySimpleDiscovery<>(discovery, reduction.compose(ignoreParameter(filterSimpleListener(predicate))));

        }

        FriendlyEnrichedDiscovery<Entity> enrich() {
            return new FriendlyEnrichedDiscovery<>(discovery, reduction.compose(ignoreParameter(Scratch.enrich())));

        }

        FriendlyDiscoveredOnlyDiscovery<Entity> discoveredOnly() {
            return new FriendlyDiscoveredOnlyDiscovery<>(discovery, reduction.compose(ignoreParameter(onlyDiscovered())));
        }
    }

    static class FriendlyDiscoveredOnlyDiscovery<Entity> {
        final Function<AbstractDiscoverer, AbstractDiscoverer> discovery;
        final Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<MetricReporter, Consumer<Entity>>>> reduction;

        FriendlyDiscoveredOnlyDiscovery(Function<AbstractDiscoverer, AbstractDiscoverer> discovery,
                                        Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<MetricReporter, Consumer<Entity>>>> reduction) {
            this.discovery = discovery;
            this.reduction = reduction;
        }

        FriendlyDiscovery readers(Function<MetricReporter, Consumer<Entity>> mnwToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.compose(omitMetricReporter())
                                                                    .compose(useMetricReporter(Verbosity.INFO))
                                                                    .transform(Scratch.readersDiscovery())
                                                                    .apply(mnw -> mnwToLsn)));
        }

        FriendlyDiscovery writers(Function<MetricReporter, Consumer<Entity>> mnwToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.compose(omitMetricReporter())
                                                                    .compose(useMetricReporter(Verbosity.INFO))
                                                                    .transform(Scratch.writersDiscovery())
                                                                    .apply(mnw -> mnwToLsn)));
        }

        <MEntity> FriendlyDiscoveredOnlyDiscovery<MEntity> map(Function<Entity, MEntity> mapper) {
            return new FriendlyDiscoveredOnlyDiscovery<>(discovery, reduction.compose(ignoreParameter(mapDiscoveredOnlyEntity(mapper))));
        }

        FriendlyDiscoveredOnlyDiscovery<Entity> filter(Predicate<Entity> filter) {
            return new FriendlyDiscoveredOnlyDiscovery<>(discovery, reduction.compose(ignoreParameter(Scratch.translate(cns -> mr -> entity -> {
                if (filter.test(entity)) {
                    cns.accept(entity);
                }
            }))));
        }

        FriendlyDiscoveredOnlyDiscovery<Entity> unduplicate() {
            return new FriendlyDiscoveredOnlyDiscovery<>(discovery, reduction.compose(ignoreParameter(discoveredOnlyUnduplicate())));
        }
    }

    static class FriendlyEnrichedDiscovery<Entity> {
        final Function<AbstractDiscoverer, AbstractDiscoverer> discovery;
        final Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<MetricReporter, EnrichedListener<Entity>>>> reduction;

        FriendlyEnrichedDiscovery(Function<AbstractDiscoverer, AbstractDiscoverer> discovery,
                                  Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<MetricReporter, EnrichedListener<Entity>>>> reduction) {
            this.discovery = discovery;
            this.reduction = reduction;
        }

        FriendlyDiscovery readers(Function<MetricReporter, EnrichedListener<Entity>> mrToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.compose(omitMetricReporter())
                                                                    .compose(useMetricReporter(Verbosity.INFO))
                                                                    .transform(Scratch.readersDiscovery())
                                                                    .apply(mnw -> mrToLsn)));
        }

        FriendlyDiscovery writers(Function<MetricReporter, EnrichedListener<Entity>> mrToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.compose(omitMetricReporter())
                                                                    .compose(useMetricReporter(Verbosity.INFO))
                                                                    .transform(Scratch.writersDiscovery())
                                                                    .apply(mnw -> mrToLsn)));
        }

        <MEntity> FriendlyEnrichedDiscovery<MEntity> map(Function<Entity, MEntity> mapper) {
            return new FriendlyEnrichedDiscovery<>(discovery, reduction.compose(ignoreParameter(mapEnrichedEntity(mapper))));
        }

        FriendlyEnrichedDiscovery<Entity> unduplicate() {
            return new FriendlyEnrichedDiscovery<>(discovery, reduction.compose(ignoreParameter(enrichedUnduplicate())));
        }
    }
}
