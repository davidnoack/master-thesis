package de.noack.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * This POJO represents the key of Centralised Securities Data Base data.
 *
 * @author davidnoack
 */
@Data
@NoArgsConstructor
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CSDBKey {
    @NonNull
    private String identifier;
    @NonNull
    private Integer period;
    @NonNull
    private Integer version;

    @Override
    public String toString() {
        return identifier
                + "+" + period
                + "+" + version;
    }
}