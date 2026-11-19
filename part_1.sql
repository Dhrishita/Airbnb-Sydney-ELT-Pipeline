/* =========================================================================
   part_1.sql
   Assignment 3: Big Data Engineering | Dhrishita Parve (26304511)

   Part 1 - Bronze layer for the Airbnb + Census ELT pipeline.
   
   Platform : Google Cloud SQL for PostgreSQL (database: postgres)
   Run from : DBeaver, connected as the postgres user
   Loaded by: Airflow DAG "airbnb_bronze_loader" on Cloud Composer

   HOW TO RUN
     1. Run SECTION 1 to SECTION 3 once, before the first DAG run.
        (Select from the top down to the end of SECTION 3, then Option+X.)
        
     2. Trigger the DAG with month_file = 05_2020.csv.
     3. Run SECTION 4 to check the load.
     4. After each Part 3 monthly load, run SECTION 5.

   DESIGN DECISIONS
     * Every column is TEXT. Bronze stores the source exactly as delivered,
       so a malformed value (e.g. a date in an unexpected format) can never
       fail a load. All typing, cleaning and validation happen in the Silver
       layer in dbt, where the rules are version-controlled and testable.
       
     * Column names are the lower-cased CSV headers, so the DAG can COPY each
       file straight into its table without a column mapping.
       
     * raw_listings has two audit columns:
         source_file - the monthly CSV the row came from. dbt derives the
                       reporting month from it, because some scrape dates
                       fall outside their file's month (e.g. 07_2020.csv
                       contains dates up to 2020-09-05).
                       
         loaded_at   - when Airflow loaded the row.
         
     * No primary keys or constraints in Bronze: raw data is accepted as-is
       and uniqueness is enforced and tested in Silver.
       
     * Scripts use IF NOT EXISTS, so re-running SECTION 1-2 is safe.
   ========================================================================= */


/* =========================================================================
   SECTION 1 - Schema
   ========================================================================= */
CREATE SCHEMA IF NOT EXISTS bronze;
COMMENT ON SCHEMA bronze IS
    'Raw data loaded by Airflow from the Composer bucket. All columns TEXT.';


/* =========================================================================
   SECTION 2 - Raw tables
   ========================================================================= */

-- 2.1 Airbnb monthly listings: one row per listing per monthly scrape.
--     12 files (05_2020.csv to 04_2021.csv), identical 22-column schema.
CREATE TABLE IF NOT EXISTS bronze.raw_listings (
    listing_id                       TEXT,
    scrape_id                        TEXT,
    scraped_date                     TEXT,
    host_id                          TEXT,
    host_name                        TEXT,
    host_since                       TEXT,
    host_is_superhost                TEXT,
    host_neighbourhood               TEXT,
    listing_neighbourhood            TEXT,
    property_type                    TEXT,
    room_type                        TEXT,
    accommodates                     TEXT,
    price                            TEXT,
    has_availability                 TEXT,
    availability_30                  TEXT,
    number_of_reviews                TEXT,
    review_scores_rating             TEXT,
    review_scores_accuracy           TEXT,
    review_scores_cleanliness        TEXT,
    review_scores_checkin            TEXT,
    review_scores_communication      TEXT,
    review_scores_value              TEXT,
    source_file                      TEXT,                                  -- e.g. 05_2020.csv
    loaded_at                        TIMESTAMP DEFAULT CURRENT_TIMESTAMP    -- load audit time
);
COMMENT ON TABLE bronze.raw_listings IS
    'Airbnb Sydney listings, one row per listing per month (May 2020 - Apr 2021).';

-- 2.2 Census 2016 G01: Selected Person Characteristics by Sex, LGA level.
--     109 columns: totals, 11 age bands, birthplace, language, education.
CREATE TABLE IF NOT EXISTS bronze.raw_census_g01 (
    lga_code_2016                    TEXT,
    tot_p_m                          TEXT,
    tot_p_f                          TEXT,
    tot_p_p                          TEXT,
    age_0_4_yr_m                     TEXT,
    age_0_4_yr_f                     TEXT,
    age_0_4_yr_p                     TEXT,
    age_5_14_yr_m                    TEXT,
    age_5_14_yr_f                    TEXT,
    age_5_14_yr_p                    TEXT,
    age_15_19_yr_m                   TEXT,
    age_15_19_yr_f                   TEXT,
    age_15_19_yr_p                   TEXT,
    age_20_24_yr_m                   TEXT,
    age_20_24_yr_f                   TEXT,
    age_20_24_yr_p                   TEXT,
    age_25_34_yr_m                   TEXT,
    age_25_34_yr_f                   TEXT,
    age_25_34_yr_p                   TEXT,
    age_35_44_yr_m                   TEXT,
    age_35_44_yr_f                   TEXT,
    age_35_44_yr_p                   TEXT,
    age_45_54_yr_m                   TEXT,
    age_45_54_yr_f                   TEXT,
    age_45_54_yr_p                   TEXT,
    age_55_64_yr_m                   TEXT,
    age_55_64_yr_f                   TEXT,
    age_55_64_yr_p                   TEXT,
    age_65_74_yr_m                   TEXT,
    age_65_74_yr_f                   TEXT,
    age_65_74_yr_p                   TEXT,
    age_75_84_yr_m                   TEXT,
    age_75_84_yr_f                   TEXT,
    age_75_84_yr_p                   TEXT,
    age_85ov_m                       TEXT,
    age_85ov_f                       TEXT,
    age_85ov_p                       TEXT,
    counted_census_night_home_m      TEXT,
    counted_census_night_home_f      TEXT,
    counted_census_night_home_p      TEXT,
    count_census_nt_ewhere_aust_m    TEXT,
    count_census_nt_ewhere_aust_f    TEXT,
    count_census_nt_ewhere_aust_p    TEXT,
    indigenous_psns_aboriginal_m     TEXT,
    indigenous_psns_aboriginal_f     TEXT,
    indigenous_psns_aboriginal_p     TEXT,
    indig_psns_torres_strait_is_m    TEXT,
    indig_psns_torres_strait_is_f    TEXT,
    indig_psns_torres_strait_is_p    TEXT,
    indig_bth_abor_torres_st_is_m    TEXT,
    indig_bth_abor_torres_st_is_f    TEXT,
    indig_bth_abor_torres_st_is_p    TEXT,
    indigenous_p_tot_m               TEXT,
    indigenous_p_tot_f               TEXT,
    indigenous_p_tot_p               TEXT,
    birthplace_australia_m           TEXT,
    birthplace_australia_f           TEXT,
    birthplace_australia_p           TEXT,
    birthplace_elsewhere_m           TEXT,
    birthplace_elsewhere_f           TEXT,
    birthplace_elsewhere_p           TEXT,
    lang_spoken_home_eng_only_m      TEXT,
    lang_spoken_home_eng_only_f      TEXT,
    lang_spoken_home_eng_only_p      TEXT,
    lang_spoken_home_oth_lang_m      TEXT,
    lang_spoken_home_oth_lang_f      TEXT,
    lang_spoken_home_oth_lang_p      TEXT,
    australian_citizen_m             TEXT,
    australian_citizen_f             TEXT,
    australian_citizen_p             TEXT,
    age_psns_att_educ_inst_0_4_m     TEXT,
    age_psns_att_educ_inst_0_4_f     TEXT,
    age_psns_att_educ_inst_0_4_p     TEXT,
    age_psns_att_educ_inst_5_14_m    TEXT,
    age_psns_att_educ_inst_5_14_f    TEXT,
    age_psns_att_educ_inst_5_14_p    TEXT,
    age_psns_att_edu_inst_15_19_m    TEXT,
    age_psns_att_edu_inst_15_19_f    TEXT,
    age_psns_att_edu_inst_15_19_p    TEXT,
    age_psns_att_edu_inst_20_24_m    TEXT,
    age_psns_att_edu_inst_20_24_f    TEXT,
    age_psns_att_edu_inst_20_24_p    TEXT,
    age_psns_att_edu_inst_25_ov_m    TEXT,
    age_psns_att_edu_inst_25_ov_f    TEXT,
    age_psns_att_edu_inst_25_ov_p    TEXT,
    high_yr_schl_comp_yr_12_eq_m     TEXT,
    high_yr_schl_comp_yr_12_eq_f     TEXT,
    high_yr_schl_comp_yr_12_eq_p     TEXT,
    high_yr_schl_comp_yr_11_eq_m     TEXT,
    high_yr_schl_comp_yr_11_eq_f     TEXT,
    high_yr_schl_comp_yr_11_eq_p     TEXT,
    high_yr_schl_comp_yr_10_eq_m     TEXT,
    high_yr_schl_comp_yr_10_eq_f     TEXT,
    high_yr_schl_comp_yr_10_eq_p     TEXT,
    high_yr_schl_comp_yr_9_eq_m      TEXT,
    high_yr_schl_comp_yr_9_eq_f      TEXT,
    high_yr_schl_comp_yr_9_eq_p      TEXT,
    high_yr_schl_comp_yr_8_belw_m    TEXT,
    high_yr_schl_comp_yr_8_belw_f    TEXT,
    high_yr_schl_comp_yr_8_belw_p    TEXT,
    high_yr_schl_comp_d_n_g_sch_m    TEXT,
    high_yr_schl_comp_d_n_g_sch_f    TEXT,
    high_yr_schl_comp_d_n_g_sch_p    TEXT,
    count_psns_occ_priv_dwgs_m       TEXT,
    count_psns_occ_priv_dwgs_f       TEXT,
    count_psns_occ_priv_dwgs_p       TEXT,
    count_persons_other_dwgs_m       TEXT,
    count_persons_other_dwgs_f       TEXT,
    count_persons_other_dwgs_p       TEXT
);
COMMENT ON TABLE bronze.raw_census_g01 IS
    'ABS 2016 Census G01 by NSW LGA. Key LGA_CODE_2016 has an LGA prefix (LGA10050).';

-- 2.3 Census 2016 G02: Selected Medians and Averages, LGA level.
CREATE TABLE IF NOT EXISTS bronze.raw_census_g02 (
    lga_code_2016                    TEXT,
    median_age_persons               TEXT,
    median_mortgage_repay_monthly    TEXT,
    median_tot_prsnl_inc_weekly      TEXT,
    median_rent_weekly               TEXT,
    median_tot_fam_inc_weekly        TEXT,
    average_num_psns_per_bedroom     TEXT,
    median_tot_hhd_inc_weekly        TEXT,
    average_household_size           TEXT
);
COMMENT ON TABLE bronze.raw_census_g02 IS
    'ABS 2016 Census G02 by NSW LGA: median age, mortgage, rent, income, household size.';

-- 2.4 LGA code to LGA name mapping (codes are plain numbers, e.g. 10050).
CREATE TABLE IF NOT EXISTS bronze.raw_lga_code (
    lga_code                         TEXT,
    lga_name                         TEXT
);
COMMENT ON TABLE bronze.raw_lga_code IS
    'NSW LGA code (numeric, no LGA prefix) to title-case LGA name.';

-- 2.5 Suburb to LGA mapping. The CSV also has 25 empty trailing columns;
--     the DAG reads only these two.
CREATE TABLE IF NOT EXISTS bronze.raw_lga_suburb (
    lga_name                         TEXT,
    suburb_name                      TEXT
);
COMMENT ON TABLE bronze.raw_lga_suburb IS
    'NSW suburb to LGA name mapping. Names are upper case.';


/* =========================================================================
   SECTION 3 - Truncating before the first DAG run (as advised in the brief)
   ========================================================================= */
TRUNCATE TABLE bronze.raw_listings,
               bronze.raw_census_g01,
               bronze.raw_census_g02,
               bronze.raw_lga_code,
               bronze.raw_lga_suburb;


/* =========================================================================
   SECTION 4 - Verifying the Part 1 load (running after the first DAG run)
   Expected: raw_listings 37,562 | raw_census_g01 132 | raw_census_g02 132
             raw_lga_code 129    | raw_lga_suburb 4,470
   ========================================================================= */
SELECT 'raw_listings'   AS table_name, COUNT(*) AS row_count FROM bronze.raw_listings
UNION ALL SELECT 'raw_census_g01', COUNT(*) FROM bronze.raw_census_g01
UNION ALL SELECT 'raw_census_g02', COUNT(*) FROM bronze.raw_census_g02
UNION ALL SELECT 'raw_lga_code',   COUNT(*) FROM bronze.raw_lga_code
UNION ALL SELECT 'raw_lga_suburb', COUNT(*) FROM bronze.raw_lga_suburb;

-- Spot-checking a few raw rows
SELECT listing_id, scraped_date, host_since, listing_neighbourhood,
       property_type, price, has_availability, source_file
FROM bronze.raw_listings
LIMIT 5;


/* =========================================================================
   SECTION 5 - Part 3 checks (running after each monthly load)
   ========================================================================= */

-- 5.1 Rows per monthly file, in load order.
--     Expected: 05_2020 37,562 | 06_2020 36,901 | 07_2020 36,057 | 08_2020 31,391
--               09_2020 34,829 | 10_2020 34,276 | 11_2020 33,795 | 12_2020 33,871
--               01_2021 33,902 | 02_2021 33,630 | 03_2021 33,229 | 04_2021 32,679
--               Total 412,122
SELECT source_file,
       COUNT(*)       AS row_count,
       MIN(loaded_at) AS loaded_at
FROM bronze.raw_listings
GROUP BY source_file
ORDER BY RIGHT(source_file, 8), source_file;   -- sorts by year, then month

-- 5.2 Duplicate check: must return 0 rows (each listing once per file).
SELECT source_file, listing_id, COUNT(*) AS copies
FROM bronze.raw_listings
GROUP BY source_file, listing_id
HAVING COUNT(*) > 1;

-- 5.3 Reference tables must not grow after the first run.
SELECT (SELECT COUNT(*) FROM bronze.raw_census_g01) AS g01,     -- 132
       (SELECT COUNT(*) FROM bronze.raw_census_g02) AS g02,     -- 132
       (SELECT COUNT(*) FROM bronze.raw_lga_code)   AS lga_code,  -- 129
       (SELECT COUNT(*) FROM bronze.raw_lga_suburb) AS lga_suburb; -- 4,470



