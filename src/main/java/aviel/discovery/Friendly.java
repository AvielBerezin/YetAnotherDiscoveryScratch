package aviel.discovery;

import aviel.AssumeWeHave;
import aviel.AssumeWeHave.EntityInfo;
import aviel.AssumeWeHave.MetricNameWrapper;
import aviel.AssumeWeHave.MetricReporter;
import aviel.discovery.DiscoveryBasics.AbstractDiscoverer;
import aviel.discovery.Listeners.SimpleListener;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static aviel.discovery.Reductions.*;

public class Friendly {
    public static FriendlyInitial friendlyDiscovery() {
        return new FriendlyInitial();
    }

    static Function<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<AbstractDiscoverer, AbstractDiscoverer>> readersDiscovery() {
        return mnwToLsn -> discoverer -> discoverer.withReaders(mnwToLsn);
    }

    static Function<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<AbstractDiscoverer, AbstractDiscoverer>> writersDiscovery() {
        return mnwToLsn -> discoverer -> discoverer.withWriters(mnwToLsn);
    }

    public static class FriendlyInitial extends FriendlySimpleDiscovery<EntityInfo> {
        private FriendlyInitial() {
            this(new DiscoveryBasics.DiscovererEmpty());
        }

        private FriendlyInitial(AbstractDiscoverer discoverer) {
            super(discoverer, useMetricReporter(AssumeWeHave.Verbosity.DEBUG));
        }

        public DiscoveryBasics.NTClosable initiate(DiscoveryBasics.Visibility visibility) {
            return discoverer.initiate(visibility);
        }
    }

    public static class FriendlyDiscovery<Listener> {
        protected final AbstractDiscoverer discoverer;
        private final Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<MetricReporter, Listener>>> reduction;

        private FriendlyDiscovery(AbstractDiscoverer discoverer,
                                  Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<MetricReporter, Listener>>> reduction) {
            this.discoverer = discoverer;
            this.reduction = reduction;
        }

        public <MListener> FriendlyDiscovery<MListener>
        compose(Reduction<Function<MetricReporter, Listener>, Function<MetricReporter, MListener>> extention) {
            return new FriendlyDiscovery<>(discoverer, reduction.compose(ignoreParameter(extention)));
        }

        public <ExtendedListener> FriendlyDiscovery<ExtendedListener>
        translate(Function<ExtendedListener, Function<MetricReporter, Listener>> extract) {
            return compose(Reductions.translate(extract));
        }

        public FriendlyInitial readers(Function<MetricReporter, Listener> mrToLsn) {
            return assignListener(readersDiscovery(), mrToLsn);
        }

        public FriendlyInitial writers(Function<MetricReporter, Listener> mrToLsn) {
            return assignListener(writersDiscovery(), mrToLsn);
        }

        private FriendlyInitial
        assignListener(Function<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<AbstractDiscoverer, AbstractDiscoverer>> discoveryListenerAssigner,
                       Function<MetricReporter, Listener> mrToLsn) {
            return new FriendlyInitial(reduction.compose(Reductions.omitMetricReporter())
                                                .compose(Reductions.useMetricReporter(AssumeWeHave.Verbosity.INFO))
                                                .transform(discoveryListenerAssigner)
                                                .apply(mnw -> mrToLsn)
                                                .apply(discoverer));
        }
    }

    public static class FriendlySimpleDiscovery<Entity> extends FriendlyDiscovery<SimpleListener<Entity>> {
        private FriendlySimpleDiscovery(AbstractDiscoverer discoverer,
                                        Reduction<Function<MetricNameWrapper, SimpleListener<EntityInfo>>, Function<MetricNameWrapper, Function<MetricReporter, SimpleListener<Entity>>>> reduction) {
            super(discoverer, reduction);
        }

        private FriendlySimpleDiscovery(FriendlyDiscovery<SimpleListener<Entity>> base) {
            super(base.discoverer, base.reduction);
        }

        public <MEntity> FriendlySimpleDiscovery<MEntity> map(Function<Entity, MEntity> mapper) {
            return new FriendlySimpleDiscovery<>(this.translate(eLsn -> mr -> translateSimpleListener(eLsn, mapper)));
        }

        public FriendlySimpleDiscovery<Entity> filter(Predicate<Entity> predicate) {
            return new FriendlySimpleDiscovery<>(this.compose(filterSimpleListener(predicate)));
        }

        public FriendlyEnrichedDiscovery<Entity> enrich() {
            return new FriendlyEnrichedDiscovery<>(this.compose(Reductions.enrich()));

        }

        public FriendlyDiscoveredOnlyDiscovery<Entity> discoveredOnly() {
            return new FriendlyDiscoveredOnlyDiscovery<>(this.compose(Reductions.onlyDiscovered()));
        }
    }

    public static class FriendlyDiscoveredOnlyDiscovery<Entity> extends FriendlyDiscovery<Consumer<Entity>> {
        private FriendlyDiscoveredOnlyDiscovery(FriendlyDiscovery<Consumer<Entity>> base) {
            super(base.discoverer, base.reduction);
        }

        public <MEntity> FriendlyDiscoveredOnlyDiscovery<MEntity> map(Function<Entity, MEntity> mapper) {
            return new FriendlyDiscoveredOnlyDiscovery<>(this.compose(Reductions.mapDiscoveredOnlyEntity(mapper)));
        }

        public FriendlyDiscoveredOnlyDiscovery<Entity> filter(Predicate<Entity> filter) {
            return new FriendlyDiscoveredOnlyDiscovery<>(this.compose(Reductions.filterDiscoveredOnlyEntity(filter)));
        }

        public FriendlyDiscoveredOnlyDiscovery<Entity> unduplicate() {
            return new FriendlyDiscoveredOnlyDiscovery<>(this.compose(Reductions.discoveredOnlyUnduplicate()));
        }
    }

    public static class FriendlyEnrichedDiscovery<Entity> extends FriendlyDiscovery<Listeners.EnrichedListener<Entity>> {
        private FriendlyEnrichedDiscovery(FriendlyDiscovery<Listeners.EnrichedListener<Entity>> base) {
            super(base.discoverer, base.reduction);
        }

        public <MEntity> FriendlyEnrichedDiscovery<MEntity> map(Function<Entity, MEntity> mapper) {
            return new FriendlyEnrichedDiscovery<>(this.compose(Reductions.mapEnrichedEntity(mapper)));
        }

        public FriendlyEnrichedDiscovery<Entity> unduplicate() {
            return new FriendlyEnrichedDiscovery<>(this.compose(Reductions.enrichedUnduplicate()));
        }
    }
}
