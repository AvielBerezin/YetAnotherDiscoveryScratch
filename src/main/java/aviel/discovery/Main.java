package aviel.discovery;

import aviel.AssumeWeHave;
import aviel.AssumeWeHave.EntityInfo;
import aviel.discovery.DiscoveryBasics.NTClosable;
import aviel.discovery.DiscoveryBasics.Visibility;
import aviel.discovery.Listeners.EnrichedListener;
import org.apache.log4j.Logger;

import java.time.Duration;

import static aviel.AssumeWeHave.loadingBar;
import static aviel.discovery.Friendly.friendlyDiscovery;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        Visibility someVisibility = () -> AssumeWeHave::createDomainParticipant;
        try (NTClosable discoveryCloser =
                     friendlyDiscovery()
                             .enrich() // add data on disconnection (ie smart)
                             .writers(metricReporter -> new EnrichedListener<>() { // assign the listener
                                 @Override
                                 public void onDiscovered(EntityInfo entity) {
                                     logger.info("+W " + entity);
                                 }

                                 @Override
                                 public void onDisconnected(EntityInfo entity) {
                                     logger.info("-W " + entity);
                                 }
                             })
                             .enrich() // add data on disconnection (ie smart)
                             .readers(metricReporter -> new EnrichedListener<>() { // assign the listener
                                 @Override
                                 public void onDiscovered(EntityInfo entity) {
                                     logger.info("+R " + entity);
                                 }

                                 @Override
                                 public void onDisconnected(EntityInfo entity) {
                                     logger.info("-R " + entity);
                                 }
                             })
                             .enrich() // add data on disconnection (ie smart)
                             .map(EntityInfo::topicName)
                             .unduplicate() // onDiscovered we only want notification for the first writer to appear on the topic (per topic)
                             //                and on disconnection we only want a notification for the last writer to leave the topic (per topic)
                             .writers(metricReporter -> new EnrichedListener<>() { // assign the listener
                                 @Override
                                 public void onDiscovered(String topicName) {
                                     logger.info("+WT " + topicName);
                                 }

                                 @Override
                                 public void onDisconnected(String topicName) {
                                     logger.info("-WT " + topicName);
                                 }
                             })
                             .discoveredOnly() // we don't care about disconnection
                             .map(EntityInfo::partition)
                             .unduplicate() // we only want a notification for the first ever reader appearing on the partition (per partition)
                             .readers(metricReporter -> partition -> logger.info("+RP " + partition)) // assign the listener
                             .initiate(someVisibility)) {
            loadingBar(100, Duration.ofSeconds(20));
        }
    }
}
