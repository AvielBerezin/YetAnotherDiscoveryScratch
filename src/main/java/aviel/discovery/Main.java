package aviel.discovery;

import aviel.AssumeWeHave;
import aviel.AssumeWeHave.EntityInfo;
import aviel.discovery.DiscoveryBasics.NTClosable;
import aviel.discovery.Listeners.EnrichedListener;
import org.apache.log4j.Logger;

import java.time.Duration;

import static aviel.AssumeWeHave.loadingBar;
import static aviel.discovery.Friendly.friendlyDiscovery;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        DiscoveryBasics.Visibility someVisibility = () -> AssumeWeHave::createDomainParticipant;
        try (NTClosable discoveryCloser =
                     friendlyDiscovery()
                             .enrich()
                             .writers(metricNameWrapper -> new EnrichedListener<>() {
                                 @Override
                                 public void onDiscovered(EntityInfo entity) {
                                     logger.info("+W " + entity);
                                 }

                                 @Override
                                 public void onDisconnected(EntityInfo entity) {
                                     logger.info("-W " + entity);
                                 }
                             })
                             .enrich()
                             .readers(metricNameWrapper -> new EnrichedListener<>() {
                                 @Override
                                 public void onDiscovered(EntityInfo entity) {
                                     logger.info("+R " + entity);
                                 }

                                 @Override
                                 public void onDisconnected(EntityInfo entity) {
                                     logger.info("-R " + entity);
                                 }
                             })
                             .enrich()
                             .map(EntityInfo::topicName)
                             .unduplicate()
                             .writers(metricNameWrapper -> new EnrichedListener<>() {
                                 @Override
                                 public void onDiscovered(String entity) {
                                     logger.info("+WT " + entity);
                                 }

                                 @Override
                                 public void onDisconnected(String entity) {
                                     logger.info("-WT " + entity);
                                 }
                             })
                             .discoveredOnly()
                             .map(EntityInfo::partition)
                             .unduplicate()
                             .readers(metricNameWrapper -> partition -> logger.info("+RP " + partition))
                             .initiate(someVisibility)) {
            loadingBar(100, Duration.ofSeconds(20));
        }
    }
}
