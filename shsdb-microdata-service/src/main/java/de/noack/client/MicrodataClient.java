package de.noack.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface MicrodataClient {
    Logger LOGGER = LoggerFactory.getLogger(MicrodataClient.class);
    String INPUT_TOPIC_NAME = "report-vanilla";
    String INPUT_SUBSCRIPTION_NAME = "report-vanilla-subscription";
    String OUTPUT_TOPIC_NAME = "report-microdata";
}