package de.noack.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Data
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Security extends CSDB {
    private String reportedNominalCurrency;
    private String reportingBasis;

    public static Security map(final CSDB csdb) {
        final Security security = new Security();
        security.setAccruedIncomeFactor(csdb.getAccruedIncomeFactor());
        security.setAccruedInterest(csdb.getAccruedInterest());
        security.setAmountOutstanding(csdb.getAmountOutstanding());
        security.setAmountOutstandingEuro(csdb.getAmountOutstandingEuro());
        security.setAssetSecuritisationType(csdb.getAssetSecuritisationType());
        security.setAveragePrice(csdb.getAveragePrice());
        security.setAveragePrice1(csdb.getAveragePrice1());
        security.setAveragePrice2(csdb.getAveragePrice2());
        security.setCfiCode(csdb.getCfiCode());
        security.setCouponDate(csdb.getCouponDate());
        security.setCouponFrequency(csdb.getCouponFrequency());
        security.setCouponRate(csdb.getCouponRate());
        security.setCouponType(csdb.getCouponType());
        security.setCsdbKey(csdb.getCsdbKey());
        security.setDebtType(csdb.getDebtType());
        security.setDerivedIncomeEuro(csdb.getDerivedIncomeEuro());
        security.setDerivedIncomeFrequency(csdb.getDerivedIncomeFrequency());
        security.setDividendAmount(csdb.getDividendAmount());
        security.setDividendCurrency(csdb.getDividendCurrency());
        security.setDividendIncomeEuro(csdb.getDividendIncomeEuro());
        security.setDividendIncomeFrequency(csdb.getDividendIncomeFrequency());
        security.setDividendSettlementDate(csdb.getDividendSettlementDate());
        security.setDivType(csdb.getDivType());
        security.setInEADB(csdb.getInEADB());
        security.setInstrumentClass(csdb.getInstrumentClass());
        security.setInstrumentClassESA95(csdb.getInstrumentClassESA95());
        security.setInstrumentSeniorityType(csdb.getInstrumentSeniorityType());
        security.setInternalOrganisationCode(csdb.getInternalOrganisationCode());
        security.setIssueDate(csdb.getIssueDate());
        security.setIssuePrice(csdb.getIssuePrice());
        security.setIssuerArea(csdb.getIssuerArea());
        security.setIssuerESA95Sector(csdb.getIssuerESA95Sector());
        security.setIssuerID(csdb.getIssuerID());
        security.setIssuerIDType(csdb.getIssuerIDType());
        security.setIssuerLEI(csdb.getIssuerLEI());
        security.setIssuerMFI(csdb.getIssuerMFI());
        security.setIssuerNACESector(csdb.getIssuerNACESector());
        security.setIssuerName(csdb.getIssuerName());
        security.setIssuerSector(csdb.getIssuerSector());
        security.setMarketCapitalisation(csdb.getMarketCapitalisation());
        security.setMarketCapitalisationEuro(csdb.getMarketCapitalisationEuro());
        security.setMaturityDate(csdb.getMaturityDate());
        security.setNominalCurrency(csdb.getNominalCurrency());
        security.setNominalValue(csdb.getNominalValue());
        security.setNumberOutstanding(csdb.getNumberOutstanding());
        security.setPoolFactor(csdb.getPoolFactor());
        security.setPriceValue(csdb.getPriceValue());
        security.setPriceValueDate(csdb.getPriceValueDate());
        security.setPriceValueType(csdb.getPriceValueType());
        security.setPrimaryAssetClassification(csdb.getPrimaryAssetClassification());
        security.setQuotationBasis(csdb.getQuotationBasis());
        security.setRedemptionPrice(csdb.getRedemptionPrice());
        security.setSecurityStatus(csdb.getSecurityStatus());
        security.setSecurityStatusDate(csdb.getSecurityStatusDate());
        security.setShortName(csdb.getShortName());
        security.setSplitDate(csdb.getSplitDate());
        security.setSplitFactor(csdb.getSplitFactor());
        security.setYieldToMaturity(csdb.getYieldToMaturity());
        return security;
    }
}