package de.noack.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * This POJO represents the key of combined reported and CSDB data.
 *
 * @author davidnoack
 */
@Data
@NoArgsConstructor
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class MicroDataKey {
    @NonNull
    private String compilingOrg;
    @NonNull
    private Integer period;
    @NonNull
    private String frequency;
    @NonNull
    private String holderSector;
    @NonNull
    private String source;
    @NonNull
    private String holderArea;
    @NonNull
    private String functionalCategory;
    @NonNull
    private String amountType;
    @NonNull
    private String valuation;
}
