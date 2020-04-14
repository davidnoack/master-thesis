package de.noack.service;

/**
 * This enum represents the possible commit log technologies which can be used to get the application running. The chosen technology depends on the
 * value set in the properties file.
 *
 * @author davidnoack
 */
public enum CommitLog {
    PULSAR,
    KAFKA
}