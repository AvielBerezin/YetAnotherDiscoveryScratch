package aviel.discovery;

import aviel.AssumeWeHave.*;
import aviel.discovery.Listeners.EnrichedListener;
import aviel.discovery.Listeners.SimpleListener;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static aviel.AssumeWeHave.*;

public class Reductions {
    public static <Listener> Reduction<Function<MetricNameWrapper, Listener>, Function<MetricNameWrapper, Function<MetricReporter, Listener>>>
    useMetricReporter(Verbosity verbosity) {
        return new Reduction<>() {
            @Override
            public <Result> Function<Function<MetricNameWrapper, Function<MetricReporter, Listener>>, Result> transform(Function<Function<MetricNameWrapper, Listener>, Result> problem) {
                return vMnwToLsn -> problem.apply(mnw -> vMnwToLsn.apply(mnw)
                                                                  .apply(createMetricReporter(mnw, verbosity)));
            }
        };
    }

    public static <Listener> Reduction<Function<MetricNameWrapper, Function<MetricReporter, Listener>>, Function<MetricNameWrapper, Listener>>
    omitMetricReporter() {
        return new Reduction<>() {
            @Override
            public <Result> Function<Function<MetricNameWrapper, Listener>, Result> transform(Function<Function<MetricNameWrapper, Function<MetricReporter, Listener>>, Result> problem) {
                return mnwToLsn -> problem.apply(mnw -> mr -> mnwToLsn.apply(mnw));
            }
        };
    }

    public static <Entity> Reduction<Function<MetricReporter, SimpleListener<Entity>>, Function<MetricReporter, Consumer<Entity>>>
    onlyDiscovered() {
        return translate(cns -> mr -> new SimpleListener<>() {
            @Override
            public void onDiscovered(Entry<Entity> entity) {
                cns.accept(entity.value());
            }

            @Override
            public void onDisconnected(InstanceHandle instanceHandle) {}
        });
    }

    public static <Entity, MEntity> Reduction<Function<MetricReporter, Consumer<Entity>>, Function<MetricReporter, Consumer<MEntity>>>
    mapDiscoveredOnlyEntity(Function<Entity, MEntity> mapper) {
        return translate(mCns -> mr -> entity -> mCns.accept(mapper.apply(entity)));
    }

    public static <Entity> Reduction<Function<MetricReporter, Consumer<Entity>>, Function<MetricReporter, Consumer<Entity>>> filterDiscoveredOnlyEntity(Predicate<Entity> filter) {
        return translate(cns -> mr -> entity -> {
            if (filter.test(entity)) {
                cns.accept(entity);
            }
        });
    }

    public static <Entity> Reduction<Function<MetricReporter, Consumer<Entity>>, Function<MetricReporter, Consumer<Entity>>>
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

    public static <Entity> Reduction<Function<MetricReporter, SimpleListener<Entity>>, Function<MetricReporter, EnrichedListener<Entity>>>
    enrich() {
        return translate(lsn -> mr -> {
            HashMap<InstanceHandle, Entry<Entity>> entries = new HashMap<>();
            return new SimpleListener<>() {
                @Override
                public void onDiscovered(Entry<Entity> entity) {
                    entries.put(entity.key(), entity);
                    lsn.onDiscovered(entity.value());
                }

                @Override
                public void onDisconnected(InstanceHandle instanceHandle) {
                    Entry<Entity> removed = entries.remove(instanceHandle);
                    lsn.onDisconnected(removed.value());
                }
            };
        });
    }

    public static <Entity, MEntity> Reduction<Function<MetricReporter, EnrichedListener<Entity>>, Function<MetricReporter, EnrichedListener<MEntity>>>
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

    public static <Entity> Reduction<Function<MetricReporter, EnrichedListener<Entity>>, Function<MetricReporter, EnrichedListener<Entity>>>
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

    public static <Entity, MEntity> Reduction<Function<MetricReporter, SimpleListener<Entity>>, Function<MetricReporter, SimpleListener<MEntity>>>
    mapSimpleListener(Function<Entity, MEntity> mapper) {
        return translate(mLsn -> mr -> new SimpleListener<>() {
            @Override
            public void onDiscovered(Entry<Entity> entity) {
                mLsn.onDiscovered(new Entry<>(entity.key(), mapper.apply(entity.value())));
            }

            @Override
            public void onDisconnected(InstanceHandle instanceHandle) {
                mLsn.onDisconnected(instanceHandle);
            }
        });
    }

    public static <Entity> Reduction<Function<MetricReporter, SimpleListener<Entity>>, Function<MetricReporter, SimpleListener<Entity>>>
    filterSimpleListener(Predicate<Entity> predicate) {
        return translate(lsn -> mr -> {
            Set<InstanceHandle> filteredIn = Collections.synchronizedSet(new HashSet<>());
            return new SimpleListener<>() {
                @Override
                public void onDiscovered(Entry<Entity> entity) {
                    if (predicate.test(entity.value())) {
                        filteredIn.add(entity.key());
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

    public static <Param, Lsn1, Lsn2> Reduction<Function<Param, Lsn1>, Function<Param, Lsn2>> ignoreParameter(Reduction<Lsn1, Lsn2> reduction) {
        return new Reduction<>() {
            @Override
            public <Result> Function<Function<Param, Lsn2>, Result> transform(Function<Function<Param, Lsn1>, Result> problem) {
                return pToLsn -> problem.apply(param -> reduction.transform(lsn1 -> lsn1)
                                                                 .apply(pToLsn.apply(param)));
            }
        };
    }

    public static <ExtendedListener, PureListener> Reduction<Function<MetricReporter, ExtendedListener>, Function<MetricReporter, PureListener>>
    translate(Function<PureListener, Function<MetricReporter, ExtendedListener>> extract) {
        return new Reduction<>() {
            @Override
            public <Result> Function<Function<MetricReporter, PureListener>, Result> transform(
                    Function<Function<MetricReporter, ExtendedListener>, Result> problem) {
                return vMnwToLsn -> problem.apply(mr -> extract.apply(vMnwToLsn.apply(mr)).apply(mr));
            }
        };
    }

    public interface Reduction<Value1, Value2> {
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
}
