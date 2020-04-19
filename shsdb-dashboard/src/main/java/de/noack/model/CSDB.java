package de.noack.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * This POJO represents Centralised Securities Data Base data.
 *
 * @author davidnoack
 */
@Data
@NoArgsConstructor
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CSDB {
    @NonNull
    private CSDBKey csdbKey;
    private BigDecimal accruedIncomeFactor;
    private BigDecimal accruedInterest;
    private BigDecimal amountOutstanding;
    private BigDecimal amountOutstandingEuro;
    private String assetSecuritisationType;
    private BigDecimal averagePrice;
    private String cfiCode;
    @JsonDeserialize(using = LocalDateDeserializer.class)
    @JsonSerialize(using = LocalDateSerializer.class)
    private LocalDate couponDate;
    private String couponFrequency;
    private BigDecimal couponRate;
    private String couponType;
    private String debtType;
    private BigDecimal dividendAmount;
    private String dividendCurrency;
    @JsonDeserialize(using = LocalDateDeserializer.class)
    @JsonSerialize(using = LocalDateSerializer.class)
    private LocalDate dividendSettlementDate;
    private String divType;
    private String inEADB;
    private String instrumentClass;
    private String instrumentClassESA95;
    private String instrumentSeniorityType;
    private String internalOrganisationCode;
    private String issuerID;
    private String issuerIDType;
    @JsonDeserialize(using = LocalDateDeserializer.class)
    @JsonSerialize(using = LocalDateSerializer.class)
    private LocalDate issueDate;
    private BigDecimal issuePrice;
    private String issuerArea;
    private String issuerLEI;
    private String issuerMFI;
    private String issuerNACESector;
    private String issuerName;
    private String issuerSector;
    private String issuerESA95Sector;
    private BigDecimal marketCapitalisation;
    private BigDecimal marketCapitalisationEuro;
    @JsonDeserialize(using = LocalDateDeserializer.class)
    @JsonSerialize(using = LocalDateSerializer.class)
    private LocalDate maturityDate;
    private String nominalCurrency;
    private BigDecimal nominalValue;
    private BigDecimal poolFactor;
    private BigDecimal priceValue;
    @JsonDeserialize(using = LocalDateDeserializer.class)
    @JsonSerialize(using = LocalDateSerializer.class)
    private LocalDate priceValueDate;
    private String priceValueType;
    private String primaryAssetClassification;
    private String quotationBasis;
    private String securityStatus;
    @JsonDeserialize(using = LocalDateDeserializer.class)
    @JsonSerialize(using = LocalDateSerializer.class)
    private LocalDate securityStatusDate;
    private String shortName;
    @JsonDeserialize(using = LocalDateDeserializer.class)
    @JsonSerialize(using = LocalDateSerializer.class)
    private LocalDate splitDate;
    private BigDecimal splitFactor;
    private BigDecimal yieldToMaturity;
    private BigDecimal derivedIncomeEuro;
    private String derivedIncomeFrequency;
    private BigDecimal dividendIncomeEuro;
    private String dividendIncomeFrequency;
    private BigDecimal redemptionPrice;
    private BigDecimal numberOutstanding;
    private BigDecimal averagePrice1;
    private BigDecimal averagePrice2;
}