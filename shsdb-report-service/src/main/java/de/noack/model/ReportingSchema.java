package de.noack.model;

/**
 * This enum represents the attributes which can be reported to SHSDB within the sector module. It is used to validate incoming report files.
 *
 * @author davidnoack
 */
public enum ReportingSchema {
    COMPILING_ORG,
    PERIOD,
    FREQ,
    ACCR_INTR_MV,
    ACCR_INTR_TX,
    EARLY_RED,
    ISIN,
    NOM_CURR,
    REPORTING_BASIS,
    HOLDER_SECTOR,
    SOURCE,
    HOLDER_AREA,
    FUNCTIONAL_CATEGORY,
    AMOUNT_TYPE,
    VALUATION,
    OBS_VALUE,
    UNIT_MEASURE,
    CONF_STATUS,
    CONF_AMOUNT
}