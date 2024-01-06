package aviel.discovery;

import aviel.AssumeWeHave.DomainParticipant;
import aviel.AssumeWeHave.EntityInfo;
import aviel.AssumeWeHave.MetricNameWrapper;
import aviel.AssumeWeHave.ParticipantParams;
import aviel.discovery.Listeners.SimpleListener;

import java.util.function.Function;

import static aviel.AssumeWeHave.getDiscoveryMetricNamesWrapper;
import static aviel.discovery.Listeners.listenersSequence;

public class DiscoveryBasics {
    interface Visibility {
        ParticipantParams createParticipantParams();
    }

    public interface NTClosable extends AutoCloseable {
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
}
