package aviel.discovery;

import aviel.AssumeWeHave.EntityInfo;
import aviel.AssumeWeHave.Entry;
import aviel.AssumeWeHave.InstanceHandle;

import java.util.function.Function;

public class Listeners {
    static <From> Function<From, SimpleListener<EntityInfo>> listenersSequence(Function<From, SimpleListener<EntityInfo>> mrToLsn1,
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

    public interface SimpleListener<Value> {
        void onDiscovered(Entry<Value> entity);
        void onDisconnected(InstanceHandle instanceHandle);
    }

    interface EnrichedListener<Value> {
        void onDiscovered(Value entity);
        void onDisconnected(Value entity);
    }
}
