package de.noack.service;

import de.noack.client.CsdbClient;
import de.noack.client.pulsar.CsdbPulsarClient;
import de.noack.model.CSDB;
import de.noack.model.CSDBKey;
import de.noack.model.CSDBSchema;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import java.io.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static de.noack.model.CSDBSchema.*;

@RequestScoped
public class CsdbService {
    private static final Logger LOGGER = LoggerFactory.getLogger(CsdbService.class);
    private static final String CSV_DELIMITER = ";";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DATE_FORMAT);
    private static final String IMPLEMENTATION_MISSING = "Not yet implemented!";
    private static final String NO_DATA = "No CSDB Data provided!";
    private CsdbClient csdbClient;

    @Inject
    public CsdbService(@ConfigProperty(name = "commitlog") final CommitLog commitLog) {
        super();
        switch (commitLog) {
            case PULSAR:
                csdbClient = new CsdbPulsarClient();
                break;
            case KAFKA:
                // TODO: Implement
                throw new RuntimeException(IMPLEMENTATION_MISSING);
            default:
                throw new RuntimeException(IMPLEMENTATION_MISSING);
        }
    }

    public String produce(byte[] csdb) throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(csdb)))) {
            final String firstLine = bufferedReader.readLine();
            if (firstLine == null) throw new RuntimeException(NO_DATA);
            final String[] csvAttributes = firstLine.split(CSV_DELIMITER);
            final Map<CSDBSchema, Integer> colummnOrder = new LinkedHashMap<>();
            for (int i = 0; i < csvAttributes.length; i++) {
                colummnOrder.put(CSDBSchema.valueOf(csvAttributes[i]), i);
            }
            final String secondLine = bufferedReader.readLine();
            if (secondLine == null) throw new RuntimeException(NO_DATA);
            String csdbKey = csdbClient.produceCsdb(createCSDBEntry(secondLine, colummnOrder));
            bufferedReader.lines().forEach(csvLine -> {
                try {
                    csdbClient.produceCsdb(createCSDBEntry(csvLine, colummnOrder));
                } catch (IOException e) {
                    LOGGER.error("Unexpected error during CSDB producing occurred. Reason: {}", e.getMessage());
                }
            });
            return csdbKey;
        }
    }

    public InputStream read(final String messageKey) throws IOException {
        return csdbClient.readCsdb(messageKey);
    }

    public Set<CSDB> consume() {
        return csdbClient.consumeCsdbs();
    }

    public CSDB createCSDBEntry(final String csvLine, final Map<CSDBSchema, Integer> columnOrder) {
        final String[] attributes = csvLine.split(CSV_DELIMITER);

        final String identifier = attributes[columnOrder.get(IDENTIFIER)].trim();
        final Integer period = Integer.valueOf(attributes[columnOrder.get(PERIOD)].trim());
        final Integer version = Integer.valueOf(attributes[columnOrder.get(VERSION)].trim());
        final CSDB csdb = new CSDB(new CSDBKey(identifier, period, version));

        final int maxArrayIndex = attributes.length - 1;

        final Integer accruedIncomeFactorColumn = columnOrder.get(ACCR_INCOME_FACTOR);
        final String accruedIncomeFactorString = accruedIncomeFactorColumn == null || accruedIncomeFactorColumn > maxArrayIndex ? null :
                attributes[accruedIncomeFactorColumn].trim();
        final BigDecimal accruedIncomeFactor = accruedIncomeFactorString != null && !accruedIncomeFactorString.isEmpty() ?
                new BigDecimal(accruedIncomeFactorString) : null;
        csdb.setAccruedIncomeFactor(accruedIncomeFactor);

        final Integer accruedInterestColumn = columnOrder.get(ACCR_INTEREST);
        final String accruedInterestString = accruedInterestColumn == null || accruedInterestColumn > maxArrayIndex ? null :
                attributes[accruedInterestColumn].trim();
        final BigDecimal accruedInterest = accruedInterestString != null && !accruedInterestString.isEmpty() ?
                new BigDecimal(accruedInterestString) : null;
        csdb.setAccruedInterest(accruedInterest);

        final Integer amountOutstandingColumn = columnOrder.get(AMOUNT_OUT);
        final String amountOutstandingString = amountOutstandingColumn == null || amountOutstandingColumn > maxArrayIndex ? null :
                attributes[amountOutstandingColumn].trim();
        final BigDecimal amountOutstanding = amountOutstandingString != null && !amountOutstandingString.isEmpty() ?
                new BigDecimal(amountOutstandingString) : null;
        csdb.setAmountOutstanding(amountOutstanding);

        final Integer amountOutstandingEuroColumn = columnOrder.get(AMOUNT_OUTST_EUR);
        final String amountOutstandingEuroString = amountOutstandingEuroColumn == null || amountOutstandingEuroColumn > maxArrayIndex ? null :
                attributes[amountOutstandingEuroColumn].trim();
        final BigDecimal amountOutstandingEuro = amountOutstandingEuroString != null && !amountOutstandingEuroString.isEmpty() ?
                new BigDecimal(amountOutstandingEuroString) : null;
        csdb.setAmountOutstandingEuro(amountOutstandingEuro);

        final Integer assetSecuritisationTypeColumn = columnOrder.get(ASSET_SECURIS_TYPE);
        final String assetSecuritisationType = assetSecuritisationTypeColumn == null || assetSecuritisationTypeColumn > maxArrayIndex ? null :
                attributes[assetSecuritisationTypeColumn].trim();
        csdb.setAssetSecuritisationType(assetSecuritisationType != null && !assetSecuritisationType.isEmpty() ? assetSecuritisationType : null);

        final Integer averagePriceColumn = columnOrder.get(AVERAGE_PRICE);
        final String averagePriceString = averagePriceColumn == null || averagePriceColumn > maxArrayIndex ? null :
                attributes[averagePriceColumn].trim();
        final BigDecimal averagePrice = averagePriceString != null && !averagePriceString.isEmpty() ? new BigDecimal(averagePriceString) : null;
        csdb.setAveragePrice(averagePrice);

        final Integer averagePrice1Column = columnOrder.get(AVERAGE_PRICE_1);
        final String averagePrice1String = averagePrice1Column == null || averagePrice1Column > maxArrayIndex ? null :
                attributes[averagePrice1Column].trim();
        final BigDecimal averagePrice1 = averagePrice1String != null && !averagePrice1String.isEmpty() ? new BigDecimal(averagePrice1String) : null;
        csdb.setAveragePrice1(averagePrice1);

        final Integer averagePrice2Column = columnOrder.get(AVERAGE_PRICE_2);
        final String averagePrice2String = averagePrice2Column == null || averagePrice2Column > maxArrayIndex ? null :
                attributes[averagePrice2Column].trim();
        final BigDecimal averagePrice2 = averagePrice2String != null && !averagePrice2String.isEmpty() ? new BigDecimal(averagePrice2String) : null;
        csdb.setAveragePrice2(averagePrice2);

        final Integer cfiColumn = columnOrder.get(CFI);
        final String cfiCode = cfiColumn == null || cfiColumn > maxArrayIndex ? null : attributes[cfiColumn].trim();
        csdb.setCfiCode(cfiCode != null && !cfiCode.isEmpty() ? cfiCode : null);

        final Integer couponDateColumn = columnOrder.get(COUPON_DT);
        final String couponDateString = couponDateColumn == null || couponDateColumn > maxArrayIndex ? null : attributes[couponDateColumn].trim();
        final LocalDate couponDate = couponDateString != null && !couponDateString.isEmpty() ? LocalDate.parse(couponDateString,
                DATE_TIME_FORMATTER) : null;
        csdb.setCouponDate(couponDate);

        final Integer couponFrequencyColumn = columnOrder.get(COUPON_FREQUENCY2);
        final String couponFrequency = couponFrequencyColumn == null || couponFrequencyColumn > maxArrayIndex ? null :
                attributes[couponFrequencyColumn].trim();
        csdb.setCouponFrequency(couponFrequency != null && !couponFrequency.isEmpty() ? couponFrequency : null);

        final Integer couponRateColumn = columnOrder.get(COUPON_RATE);
        final String couponRateString = couponRateColumn == null || couponRateColumn > maxArrayIndex ? null :
                attributes[couponRateColumn].trim();
        final BigDecimal couponRate = couponRateString != null && !couponRateString.isEmpty() ? new BigDecimal(couponRateString) : null;
        csdb.setCouponRate(couponRate);

        final Integer couponTypeColumn = columnOrder.get(COUPON_TYPE2);
        final String couponType = couponTypeColumn == null || couponTypeColumn > maxArrayIndex ? null : attributes[couponTypeColumn].trim();
        csdb.setCouponType(couponType != null && !couponType.isEmpty() ? couponType : null);

        final Integer debtTypeColumn = columnOrder.get(DEBT_TYPE2);
        final String debtType = debtTypeColumn == null || debtTypeColumn > maxArrayIndex ? null : attributes[debtTypeColumn].trim();
        csdb.setDebtType(debtType != null && !debtType.isEmpty() ? debtType : null);

        final Integer derivedIncomeEuroColumn = columnOrder.get(DERIVED_INCOME_EUR);
        final String derivedIncomeEuroString = derivedIncomeEuroColumn == null || derivedIncomeEuroColumn > maxArrayIndex ? null :
                attributes[derivedIncomeEuroColumn].trim();
        final BigDecimal derivedIncomeEuro = derivedIncomeEuroString != null && !derivedIncomeEuroString.isEmpty() ?
                new BigDecimal(derivedIncomeEuroString) : null;
        csdb.setDerivedIncomeEuro(derivedIncomeEuro);

        final Integer derivedIncomeFrequencyColumn = columnOrder.get(DERIVED_INCOME_FREQ);
        final String derivedIncomeFrequency = derivedIncomeFrequencyColumn == null || derivedIncomeFrequencyColumn > maxArrayIndex ? null :
                attributes[derivedIncomeFrequencyColumn].trim();
        csdb.setDerivedIncomeFrequency(derivedIncomeFrequency != null && !derivedIncomeFrequency.isEmpty() ? derivedIncomeFrequency : null);

        final Integer dividendAmountColumn = columnOrder.get(DIV_AMOUNT);
        final String dividendAmountString = dividendAmountColumn == null || dividendAmountColumn > maxArrayIndex ? null :
                attributes[dividendAmountColumn].trim();
        final BigDecimal dividendAmount = dividendAmountString != null && !dividendAmountString.isEmpty() ? new BigDecimal(dividendAmountString) :
                null;
        csdb.setDividendAmount(dividendAmount);

        final Integer dividendCurrencyColumn = columnOrder.get(DIV_CURRENCY);
        final String dividendCurrency = dividendCurrencyColumn == null || dividendCurrencyColumn > maxArrayIndex ? null :
                attributes[dividendCurrencyColumn].trim();
        csdb.setDividendCurrency(dividendCurrency != null && !dividendCurrency.isEmpty() ? dividendCurrency : null);

        final Integer dividendIncomeEuroColumn = columnOrder.get(DIV_INCOME_EUR);
        final String dividendIncomeEuroString = dividendIncomeEuroColumn == null || dividendIncomeEuroColumn > maxArrayIndex ? null :
                attributes[dividendIncomeEuroColumn].trim();
        final BigDecimal dividendIncomeEuro = dividendIncomeEuroString != null && !dividendIncomeEuroString.isEmpty() ?
                new BigDecimal(dividendIncomeEuroString) : null;
        csdb.setDividendIncomeEuro(dividendIncomeEuro);

        final Integer dividendIncomeFrequencyColumn = columnOrder.get(DIV_FREQ);
        final String dividendIncomeFrequency = dividendIncomeFrequencyColumn == null || dividendIncomeFrequencyColumn > maxArrayIndex ? null :
                attributes[dividendIncomeFrequencyColumn].trim();
        csdb.setDividendIncomeFrequency(dividendIncomeFrequency != null && !dividendIncomeFrequency.isEmpty() ? dividendIncomeFrequency : null);

        final Integer dividendSettlementDateColumn = columnOrder.get(DIV_DT);
        final String dividendSettlementDateString = dividendSettlementDateColumn == null || dividendSettlementDateColumn > maxArrayIndex ? null :
                attributes[dividendSettlementDateColumn].trim();
        final LocalDate dividendSettlementDate = dividendSettlementDateString != null && !dividendSettlementDateString.isEmpty() ?
                LocalDate.parse(dividendSettlementDateString, DATE_TIME_FORMATTER) : null;
        csdb.setDividendSettlementDate(dividendSettlementDate);

        final Integer divTypeColumn = columnOrder.get(DIV_TYPE);
        final String divType = divTypeColumn == null || divTypeColumn > maxArrayIndex ? null : attributes[divTypeColumn].trim();
        csdb.setDivType(divType != null && !divType.isEmpty() ? divType : null);

        final Integer inEADBColumn = columnOrder.get(IN_EADB);
        final String inEADB = inEADBColumn == null || inEADBColumn > maxArrayIndex ? null : attributes[inEADBColumn].trim();
        csdb.setInEADB(inEADB != null && !inEADB.isEmpty() ? inEADB : null);

        final Integer instrumentClassColumn = columnOrder.get(ESA_INS_2010);
        final String instrumentClass = instrumentClassColumn == null || instrumentClassColumn > maxArrayIndex ? null :
                attributes[instrumentClassColumn].trim();
        csdb.setInstrumentClass(instrumentClass != null && !instrumentClass.isEmpty() ? instrumentClass : null);

        final Integer instrumentClassESA95Column = columnOrder.get(ESA_INS);
        final String instrumentClassESA95 = instrumentClassESA95Column == null || instrumentClassESA95Column > maxArrayIndex ? null :
                attributes[instrumentClassESA95Column].trim();
        csdb.setInstrumentClassESA95(instrumentClassESA95 != null && !instrumentClassESA95.isEmpty() ? instrumentClassESA95 : null);

        final Integer instrumentSeniorityTypeColumn = columnOrder.get(INS_SENIOR_TYPE);
        final String instrumentSeniorityType = instrumentSeniorityTypeColumn == null || instrumentSeniorityTypeColumn > maxArrayIndex ? null :
                attributes[instrumentSeniorityTypeColumn].trim();
        csdb.setInstrumentSeniorityType(instrumentSeniorityType != null && !instrumentSeniorityType.isEmpty() ? instrumentSeniorityType : null);

        final Integer internalOrganisationCodeColumn = columnOrder.get(INT_ORG_CODE);
        final String internalOrganisationCode = internalOrganisationCodeColumn == null || internalOrganisationCodeColumn > maxArrayIndex ? null :
                attributes[internalOrganisationCodeColumn].trim();
        csdb.setInternalOrganisationCode(internalOrganisationCode != null && !internalOrganisationCode.isEmpty() ? internalOrganisationCode : null);

        final Integer issueDateColumn = columnOrder.get(ISSUE_DT);
        final String issueDateString = issueDateColumn == null || issueDateColumn > maxArrayIndex ? null : attributes[issueDateColumn].trim();
        final LocalDate issueDate = issueDateString != null && !issueDateString.isEmpty() ? LocalDate.parse(issueDateString,
                DATE_TIME_FORMATTER) : null;
        csdb.setIssueDate(issueDate);

        final Integer issuePriceColumn = columnOrder.get(ISSUE_PRICE);
        final String issuePriceString = issuePriceColumn == null || issuePriceColumn > maxArrayIndex ? null :
                attributes[issuePriceColumn].trim();
        final BigDecimal issuePrice = issuePriceString != null && !issuePriceString.isEmpty() ? new BigDecimal(issuePriceString) : null;
        csdb.setIssuePrice(issuePrice);

        final Integer issuerAreaColumn = columnOrder.get(ISSUER_COUNTRY);
        final String issuerArea = issuerAreaColumn == null || issuerAreaColumn > maxArrayIndex ? null : attributes[issuerAreaColumn].trim();
        csdb.setIssuerArea(issuerArea != null && !issuerArea.isEmpty() ? issuerArea : null);

        final Integer issuerESA95SectorColumn = columnOrder.get(ESA_ISSUER);
        final String issuerESA95Sector = issuerESA95SectorColumn == null || issuerESA95SectorColumn > maxArrayIndex ? null :
                attributes[issuerESA95SectorColumn].trim();
        csdb.setIssuerESA95Sector(issuerESA95Sector != null && !issuerESA95Sector.isEmpty() ? issuerESA95Sector : null);

        final Integer issuerIDColumn = columnOrder.get(ISS_ID);
        final String issuerID = issuerIDColumn == null || issuerIDColumn > maxArrayIndex ? null : attributes[issuerIDColumn].trim();
        csdb.setIssuerID(issuerID != null && !issuerID.isEmpty() ? issuerID : null);

        final Integer issuerIDTypeColumn = columnOrder.get(ISS_ID_TYPE);
        final String issuerIDType = issuerIDTypeColumn == null || issuerIDTypeColumn > maxArrayIndex ? null : attributes[issuerIDTypeColumn].trim();
        csdb.setIssuerIDType(issuerIDType != null && !issuerIDType.isEmpty() ? issuerIDType : null);

        final Integer issuerLEIColumn = columnOrder.get(LEI);
        final String issuerLEI = issuerLEIColumn == null || issuerLEIColumn > maxArrayIndex ? null : attributes[issuerLEIColumn].trim();
        csdb.setIssuerLEI(issuerLEI != null && !issuerLEI.isEmpty() ? issuerLEI : null);

        final Integer issuerMFIColumn = columnOrder.get(MFI);
        final String issuerMFI = issuerMFIColumn == null || issuerMFIColumn > maxArrayIndex ? null : attributes[issuerMFIColumn].trim();
        csdb.setIssuerMFI(issuerMFI != null && !issuerMFI.isEmpty() ? issuerMFI : null);

        final Integer issuerNACESectorColumn = columnOrder.get(NACE);
        final String issuerNACESector = issuerNACESectorColumn == null || issuerNACESectorColumn > maxArrayIndex ? null :
                attributes[issuerNACESectorColumn].trim();
        csdb.setIssuerNACESector(issuerNACESector != null && !issuerNACESector.isEmpty() ? issuerNACESector : null);

        final Integer issuerNameColumn = columnOrder.get(ISSUER_NAME);
        final String issuerName = issuerNameColumn == null || issuerNameColumn > maxArrayIndex ? null : attributes[issuerNameColumn].trim();
        csdb.setIssuerName(issuerName != null && !issuerName.isEmpty() ? issuerName : null);

        final Integer issuerSectorColumn = columnOrder.get(ESA_ISSUER_2010);
        final String issuerSector = issuerSectorColumn == null || issuerSectorColumn > maxArrayIndex ? null : attributes[issuerSectorColumn].trim();
        csdb.setIssuerSector(issuerSector != null && !issuerSector.isEmpty() ? issuerSector : null);

        final Integer marketCapitalisationColumn = columnOrder.get(MARKET_CAPITAL);
        final String marketCapitalisationString = marketCapitalisationColumn == null || marketCapitalisationColumn > maxArrayIndex ? null :
                attributes[marketCapitalisationColumn].trim();
        final BigDecimal marketCapitalisation = marketCapitalisationString != null && !marketCapitalisationString.isEmpty() ?
                new BigDecimal(marketCapitalisationString) : null;
        csdb.setMarketCapitalisation(marketCapitalisation);

        final Integer marketCapitalisationEuroColumn = columnOrder.get(MARKET_CAP_EUR);
        final String marketCapitalisationEuroString = marketCapitalisationEuroColumn == null || marketCapitalisationEuroColumn > maxArrayIndex ?
                null :
                attributes[marketCapitalisationEuroColumn].trim();
        final BigDecimal marketCapitalisationEuro = marketCapitalisationEuroString != null && !marketCapitalisationEuroString.isEmpty() ?
                new BigDecimal(marketCapitalisationEuroString) : null;
        csdb.setMarketCapitalisationEuro(marketCapitalisationEuro);

        final Integer maturityDateColumn = columnOrder.get(MATURITY_DT);
        final String maturityDateString = maturityDateColumn == null || maturityDateColumn > maxArrayIndex ? null :
                attributes[maturityDateColumn].trim();
        final LocalDate maturityDate = maturityDateString != null && !maturityDateString.isEmpty() ? LocalDate.parse(maturityDateString,
                DATE_TIME_FORMATTER) : null;
        csdb.setMaturityDate(maturityDate);

        final Integer nominalCurrencyColumn = columnOrder.get(NOMINAL_CURRENCY);
        final String nominalCurrency = nominalCurrencyColumn == null || nominalCurrencyColumn > maxArrayIndex ? null :
                attributes[nominalCurrencyColumn].trim();
        csdb.setNominalCurrency(nominalCurrency != null && !nominalCurrency.isEmpty() ? nominalCurrency : null);

        final Integer nominalValueColumn = columnOrder.get(NOMINAL_VALUE);
        final String nominalValueString = nominalValueColumn == null || nominalValueColumn > maxArrayIndex ? null :
                attributes[nominalValueColumn].trim();
        final BigDecimal nominalValue = nominalValueString != null && !nominalValueString.isEmpty() ? new BigDecimal(nominalValueString) : null;
        csdb.setNominalValue(nominalValue);

        final Integer numberOutstandingColumn = columnOrder.get(NUMBER_OUTST);
        final String numberOutstandingString = numberOutstandingColumn == null || numberOutstandingColumn > maxArrayIndex ? null :
                attributes[numberOutstandingColumn].trim();
        final BigDecimal numberOutstanding = numberOutstandingString != null && !numberOutstandingString.isEmpty() ?
                new BigDecimal(numberOutstandingString) : null;
        csdb.setNumberOutstanding(numberOutstanding);

        final Integer poolFactorColumn = columnOrder.get(POOL_FACTOR);
        final String poolFactorString = poolFactorColumn == null || poolFactorColumn > maxArrayIndex ? null :
                attributes[poolFactorColumn].trim();
        final BigDecimal poolFactor = poolFactorString != null && !poolFactorString.isEmpty() ? new BigDecimal(poolFactorString) : null;
        csdb.setPoolFactor(poolFactor);

        final Integer priceValueColumn = columnOrder.get(PRICE);
        final String priceValueString = priceValueColumn == null || priceValueColumn > maxArrayIndex ? null :
                attributes[priceValueColumn].trim();
        final BigDecimal priceValue = priceValueString != null && !priceValueString.isEmpty() ? new BigDecimal(priceValueString) : null;
        csdb.setPriceValue(priceValue);

        final Integer priceValueDateColumn = columnOrder.get(PRICE_DT);
        final String priceValueDateString = priceValueDateColumn == null || priceValueDateColumn > maxArrayIndex ? null :
                attributes[priceValueDateColumn].trim();
        final LocalDate priceValueDate = priceValueDateString != null && !priceValueDateString.isEmpty() ?
                LocalDate.parse(priceValueDateString, DATE_TIME_FORMATTER) : null;
        csdb.setPriceValueDate(priceValueDate);

        final Integer priceValueTypeColumn = columnOrder.get(PRICE_VT);
        final String priceValueType = priceValueTypeColumn == null || priceValueTypeColumn > maxArrayIndex ? null :
                attributes[priceValueTypeColumn].trim();
        csdb.setPriceValueType(priceValueType != null && !priceValueType.isEmpty() ? priceValueType : null);

        final Integer primaryAssetClassificationColumn = columnOrder.get(PRIMARY_ASSET_CLASS);
        final String primaryAssetClassification = primaryAssetClassificationColumn == null || primaryAssetClassificationColumn > maxArrayIndex ?
                null : attributes[primaryAssetClassificationColumn].trim();
        csdb.setPrimaryAssetClassification(primaryAssetClassification != null && !primaryAssetClassification.isEmpty() ?
                primaryAssetClassification : null);

        final Integer quotationBasisColumn = columnOrder.get(QUOTATION_BASIS);
        final String quotationBasis = quotationBasisColumn == null || quotationBasisColumn > maxArrayIndex ? null :
                attributes[quotationBasisColumn].trim();
        csdb.setQuotationBasis(quotationBasis != null && !quotationBasis.isEmpty() ? quotationBasis : null);

        final Integer redemptionPriceColumn = columnOrder.get(REDEMPTION_PRICE);
        final String redemptionPriceString = redemptionPriceColumn == null || redemptionPriceColumn > maxArrayIndex ? null :
                attributes[redemptionPriceColumn].trim();
        final BigDecimal redemptionPrice = redemptionPriceString != null && !redemptionPriceString.isEmpty() ?
                new BigDecimal(redemptionPriceString) : null;
        csdb.setRedemptionPrice(redemptionPrice);

        final Integer securityStatusColumn = columnOrder.get(SEC_STATUS);
        final String securityStatus = securityStatusColumn == null || securityStatusColumn > maxArrayIndex ? null :
                attributes[securityStatusColumn].trim();
        csdb.setSecurityStatus(securityStatus != null && !securityStatus.isEmpty() ? securityStatus : null);

        final Integer securityStatusDateColumn = columnOrder.get(SEC_STATUS_DT);
        final String securityStatusDateString = securityStatusDateColumn == null || securityStatusDateColumn > maxArrayIndex ? null :
                attributes[securityStatusDateColumn].trim();
        final LocalDate securityStatusDate = securityStatusDateString != null && !securityStatusDateString.isEmpty() ?
                LocalDate.parse(securityStatusDateString, DATE_TIME_FORMATTER) : null;
        csdb.setSecurityStatusDate(securityStatusDate);

        final Integer shortNameColumn = columnOrder.get(SHORT_NAME);
        final String shortName = shortNameColumn == null || shortNameColumn > maxArrayIndex ? null : attributes[shortNameColumn].trim();
        csdb.setShortName(shortName != null && !shortName.isEmpty() ? shortName : null);

        final Integer splitDateColumn = columnOrder.get(SPLIT_DT);
        final String splitDateString = splitDateColumn == null || splitDateColumn > maxArrayIndex ? null : attributes[splitDateColumn].trim();
        final LocalDate splitDate = splitDateString != null && !splitDateString.isEmpty() ? LocalDate.parse(splitDateString, DATE_TIME_FORMATTER) :
                null;
        csdb.setSplitDate(splitDate);

        final Integer splitFactorColumn = columnOrder.get(SPLIT_FAC);
        final String splitFactorString = splitFactorColumn == null || splitFactorColumn > maxArrayIndex ? null :
                attributes[splitFactorColumn].trim();
        final BigDecimal splitFactor = splitFactorString != null && !splitFactorString.isEmpty() ? new BigDecimal(splitFactorString) : null;
        csdb.setSplitFactor(splitFactor);

        final Integer yieldToMaturityColumn = columnOrder.get(YIELD);
        final String yieldToMaturityString = yieldToMaturityColumn == null || yieldToMaturityColumn > maxArrayIndex ? null :
                attributes[yieldToMaturityColumn].trim();
        final BigDecimal yieldToMaturity = yieldToMaturityString != null && !yieldToMaturityString.isEmpty() ?
                new BigDecimal(yieldToMaturityString) : null;
        csdb.setYieldToMaturity(yieldToMaturity);

        return csdb;
    }
}