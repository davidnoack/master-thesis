package de.noack.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * This class represents the unique key of one dataset of reported holdings data. All attributes are mandatory.
 *
 * @author davidnoack
 */
@Data
@NoArgsConstructor
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ReportedDataKey {
    @NonNull
    private String compilingOrg;
    @NonNull
    private Integer period;
    @NonNull
    private String frequency;
    @NonNull
    private String isin;
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

    @Override
    public String toString() {
        return compilingOrg
                + "+" + period
                + "+" + frequency
                + "+" + isin
                + "+" + holderSector
                + "+" + source
                + "+" + holderArea
                + "+" + functionalCategory
                + "+" + amountType
                + "+" + valuation;
    }
}