package aviel.discovery;

import aviel.AssumeWeHave;
import aviel.AssumeWeHave.EntityInfo;
import aviel.AssumeWeHave.MetricNameWrapper;
import aviel.discovery.DiscoveryBasics.AbstractDiscoverer;
import aviel.discovery.Listeners.SimpleListener;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static aviel.discovery.Reductions.*;

public class Friendly {
    public static FriendlyDiscovery friendlyDiscovery() {
        return new FriendlyDiscovery();
    }

    static Function<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<AbstractDiscoverer, AbstractDiscoverer>> readersDiscovery() {
        return mnwToLsn -> discoverer -> discoverer.withReaders(mnwToLsn);
    }

    static Function<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<AbstractDiscoverer, AbstractDiscoverer>> writersDiscovery() {
        return mnwToLsn -> discoverer -> discoverer.withWriters(mnwToLsn);
    }

    public static class FriendlyDiscovery extends FriendlySimpleDiscovery<EntityInfo> {
        private FriendlyDiscovery() {
            this(Function.identity());
        }

        private FriendlyDiscovery(Function<AbstractDiscoverer, AbstractDiscoverer> discovery) {
            super(discovery, Reductions.useMetricReporter(AssumeWeHave.Verbosity.DEBUG));
        }

        DiscoveryBasics.NTClosable initiate(DiscoveryBasics.Visibility visibility) {
            return discovery.apply(new DiscoveryBasics.DiscovererEmpty()).initiate(visibility);
        }
    }

    public static class FriendlySimpleDiscovery<Entity> {
        final Function<AbstractDiscoverer, AbstractDiscoverer> discovery;
        final Reductions.Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<AssumeWeHave.MetricReporter, SimpleListener<Entity>>>> reduction;

        private FriendlySimpleDiscovery(Function<AbstractDiscoverer, AbstractDiscoverer> discovery,
                                        Reductions.Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<AssumeWeHave.MetricReporter, SimpleListener<Entity>>>> reduction) {
            this.discovery = discovery;
            this.reduction = reduction;
        }

        public FriendlyDiscovery readers(Function<AssumeWeHave.MetricReporter, SimpleListener<Entity>> mrToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.compose(Reductions.omitMetricReporter())
                                                                    .compose(Reductions.useMetricReporter(AssumeWeHave.Verbosity.INFO))
                                                                    .transform(readersDiscovery())
                                                                    .apply(mnw -> mrToLsn)));
        }

        public FriendlyDiscovery writers(Function<AssumeWeHave.MetricReporter, SimpleListener<Entity>> mrToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.compose(Reductions.omitMetricReporter())
                                                                    .compose(Reductions.useMetricReporter(AssumeWeHave.Verbosity.INFO))
                                                                    .transform(writersDiscovery())
                                                                    .apply(mnw -> mrToLsn)));
        }

        public <MEntity> FriendlySimpleDiscovery<MEntity> map(Function<Entity, MEntity> mapper) {
            return new FriendlySimpleDiscovery<>(discovery, reduction.compose(ignoreParameter(mapSimpleListener(mapper))));
        }

        public FriendlySimpleDiscovery<Entity> filter(Predicate<Entity> predicate) {
            return new FriendlySimpleDiscovery<>(discovery, reduction.compose(ignoreParameter(filterSimpleListener(predicate))));

        }

        public FriendlyEnrichedDiscovery<Entity> enrich() {
            return new FriendlyEnrichedDiscovery<>(discovery, reduction.compose(ignoreParameter(Reductions.enrich())));

        }

        public FriendlyDiscoveredOnlyDiscovery<Entity> discoveredOnly() {
            return new FriendlyDiscoveredOnlyDiscovery<>(discovery, reduction.compose(ignoreParameter(Reductions.onlyDiscovered())));
        }
    }

    public static class FriendlyDiscoveredOnlyDiscovery<Entity> {
        final Function<AbstractDiscoverer, AbstractDiscoverer> discovery;
        final Reductions.Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<AssumeWeHave.MetricReporter, Consumer<Entity>>>> reduction;

        private FriendlyDiscoveredOnlyDiscovery(Function<AbstractDiscoverer, AbstractDiscoverer> discovery,
                                                Reductions.Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<AssumeWeHave.MetricReporter, Consumer<Entity>>>> reduction) {
            this.discovery = discovery;
            this.reduction = reduction;
        }

        public FriendlyDiscovery readers(Function<AssumeWeHave.MetricReporter, Consumer<Entity>> mnwToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.compose(Reductions.omitMetricReporter())
                                                                    .compose(Reductions.useMetricReporter(AssumeWeHave.Verbosity.INFO))
                                                                    .transform(readersDiscovery())
                                                                    .apply(mnw -> mnwToLsn)));
        }

        public FriendlyDiscovery writers(Function<AssumeWeHave.MetricReporter, Consumer<Entity>> mnwToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.compose(Reductions.omitMetricReporter())
                                                                    .compose(Reductions.useMetricReporter(AssumeWeHave.Verbosity.INFO))
                                                                    .transform(writersDiscovery())
                                                                    .apply(mnw -> mnwToLsn)));
        }

        public <MEntity> FriendlyDiscoveredOnlyDiscovery<MEntity> map(Function<Entity, MEntity> mapper) {
            return new FriendlyDiscoveredOnlyDiscovery<>(discovery, reduction.compose(ignoreParameter(Reductions.mapDiscoveredOnlyEntity(mapper))));
        }

        public FriendlyDiscoveredOnlyDiscovery<Entity> filter(Predicate<Entity> filter) {
            return new FriendlyDiscoveredOnlyDiscovery<>(discovery, reduction.compose(ignoreParameter(Reductions.filterDiscoveredOnlyEntity(filter))));
        }

        public FriendlyDiscoveredOnlyDiscovery<Entity> unduplicate() {
            return new FriendlyDiscoveredOnlyDiscovery<>(discovery, reduction.compose(ignoreParameter(Reductions.discoveredOnlyUnduplicate())));
        }
    }

    public static class FriendlyEnrichedDiscovery<Entity> {
        final Function<AbstractDiscoverer, AbstractDiscoverer> discovery;
        final Reductions.Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<AssumeWeHave.MetricReporter, Listeners.EnrichedListener<Entity>>>> reduction;

        private FriendlyEnrichedDiscovery(Function<AbstractDiscoverer, AbstractDiscoverer> discovery,
                                          Reductions.Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<AssumeWeHave.MetricReporter, Listeners.EnrichedListener<Entity>>>> reduction) {
            this.discovery = discovery;
            this.reduction = reduction;
        }

        public FriendlyDiscovery readers(Function<AssumeWeHave.MetricReporter, Listeners.EnrichedListener<Entity>> mrToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.compose(Reductions.omitMetricReporter())
                                                                    .compose(Reductions.useMetricReporter(AssumeWeHave.Verbosity.INFO))
                                                                    .transform(readersDiscovery())
                                                                    .apply(mnw -> mrToLsn)));
        }

        public FriendlyDiscovery writers(Function<AssumeWeHave.MetricReporter, Listeners.EnrichedListener<Entity>> mrToLsn) {
            return new FriendlyDiscovery(discovery.andThen(reduction.compose(Reductions.omitMetricReporter())
                                                                    .compose(Reductions.useMetricReporter(AssumeWeHave.Verbosity.INFO))
                                                                    .transform(writersDiscovery())
                                                                    .apply(mnw -> mrToLsn)));
        }

        public <MEntity> FriendlyEnrichedDiscovery<MEntity> map(Function<Entity, MEntity> mapper) {
            return new FriendlyEnrichedDiscovery<>(discovery, reduction.compose(ignoreParameter(Reductions.mapEnrichedEntity(mapper))));
        }

        public FriendlyEnrichedDiscovery<Entity> unduplicate() {
            return new FriendlyEnrichedDiscovery<>(discovery, reduction.compose(ignoreParameter(Reductions.enrichedUnduplicate())));
        }
    }
}
