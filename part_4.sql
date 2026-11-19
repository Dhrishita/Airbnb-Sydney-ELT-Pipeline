/* =========================================================================
   part_4.sql
   
   Assignment 3: Big Data Engineering | Dhrishita Parve (26304511)

   Part 4 - Ad-hoc analysis on the Gold layer (Cloud SQL PostgreSQL).

 

   DEFINITIONS (as in the brief and the datamart)
     Active listing          has_availability = 't'
     Stays                   30 - availability_30            (active only)
     Estimated revenue       stays * price                   (active only)
     Revenue per active      AVG(estimated_revenue) over active
     listing                 listing-months, i.e. a MONTHLY figure
     Period                  the full 12 months loaded (May 2020 - Apr 2021)

   SCD2 POINT-IN-TIME JOINS
     Every fact row is joined to the dimension version valid in its
     reporting month:
         f.month_year >= d.valid_from AND f.month_year < d.valid_to
     so a listing that moved LGA, or changed property type, is counted
     under the value it had in that month.

   INTERPRETATION NOTES
     * "Top / lowest performing LGAs" = top 3 and bottom 3 by revenue
       per active listing; "top listing_neighbourhoods" = top 5.
       
     * listing_neighbourhood holds the LGA name of the listing, so it is
       used as the listing's LGA throughout.
   ========================================================================= */


/* =========================================================================
   (a) What are the demographic differences (e.g., age group distribution, household size) between the top 13 performing 
       and lowest 13 performing LGAs based on estimated revenue per active listing over the last 12 months?
   ========================================================================= */

-- (a1) All LGAs ranked by average estimated revenue per active listing
WITH listing_lga AS (
    SELECT f.listing_id, f.month_year, f.has_availability, f.estimated_revenue,
           n.listing_neighbourhood AS lga_name, n.lga_code
    FROM gold.fact_listings f
    JOIN gold.dim_listing_neighbourhood n
      ON  f.listing_id = n.listing_id
      AND f.month_year >= n.valid_from
      AND f.month_year <  n.valid_to
)
SELECT RANK() OVER (ORDER BY AVG(estimated_revenue) DESC)  AS revenue_rank,
       lga_name,
       ROUND(AVG(estimated_revenue), 0)                    AS avg_revenue_per_active_listing,
       COUNT(*)                                            AS active_listing_months,
       COUNT(DISTINCT listing_id)                          AS distinct_listings
FROM listing_lga
WHERE has_availability = 't'
GROUP BY lga_name
ORDER BY revenue_rank;



-- (a2) Census profile of the top 3 and bottom 3 LGAs
--      Age bands are shares of total persons (G01); medians from G02.
WITH listing_lga AS (
    SELECT f.listing_id, f.has_availability, f.estimated_revenue,
           n.listing_neighbourhood AS lga_name, n.lga_code
    FROM gold.fact_listings f
    JOIN gold.dim_listing_neighbourhood n
      ON  f.listing_id = n.listing_id
      AND f.month_year >= n.valid_from
      AND f.month_year <  n.valid_to
),
lga_rev AS (
    SELECT lga_name, lga_code,
           AVG(estimated_revenue) AS avg_rev
    FROM listing_lga
    WHERE has_availability = 't'
    GROUP BY lga_name, lga_code
),
ranked AS (
    SELECT *,
           RANK() OVER (ORDER BY avg_rev DESC) AS top_rank,
           RANK() OVER (ORDER BY avg_rev ASC)  AS bottom_rank
    FROM lga_rev
)
SELECT CASE WHEN r.top_rank <= 3 THEN 'Top 3' ELSE 'Bottom 3' END                 AS lga_group,
       r.lga_name,
       ROUND(r.avg_rev, 0)                                                         AS avg_revenue_per_active_listing,
       g1.tot_p_p                                                                  AS population,
       ROUND(100.0 * (g1.age_0_4_yr_p + g1.age_5_14_yr_p) / g1.tot_p_p, 1)         AS pct_age_0_14,
       ROUND(100.0 * (g1.age_15_19_yr_p + g1.age_20_24_yr_p) / g1.tot_p_p, 1)      AS pct_age_15_24,
       ROUND(100.0 * (g1.age_25_34_yr_p + g1.age_35_44_yr_p) / g1.tot_p_p, 1)      AS pct_age_25_44,
       ROUND(100.0 * (g1.age_45_54_yr_p + g1.age_55_64_yr_p) / g1.tot_p_p, 1)      AS pct_age_45_64,
       ROUND(100.0 * (g1.age_65_74_yr_p + g1.age_75_84_yr_p + g1.age_85ov_p)
                   / g1.tot_p_p, 1)                                                AS pct_age_65_plus,
       g2.median_age_persons,
       g2.average_household_size,
       g2.median_tot_hhd_inc_weekly                                                AS median_household_income_weekly,
       g2.median_rent_weekly,
       g2.median_mortgage_repay_monthly
FROM ranked r
JOIN gold.dim_census_g01 g1 ON r.lga_code = g1.lga_code
JOIN gold.dim_census_g02 g2 ON r.lga_code = g2.lga_code
WHERE r.top_rank <= 3 OR r.bottom_rank <= 3
ORDER BY r.avg_rev DESC;


/* =========================================================================
   (b) Is there a correlation between the median age of a neighbourhood (from Census data) and the revenue generated per 
       active listing in that neighbourhood?
   ========================================================================= */

-- (b1) Correlation coefficients across all LGAs
--      Pearson r and R-squared; Spearman rho computed as the Pearson
--      correlation of average ranks (robust to the Mosman outlier).
--      Income and household size are included as comparison drivers.
WITH listing_lga AS (
    SELECT f.has_availability, f.estimated_revenue, n.lga_code
    FROM gold.fact_listings f
    JOIN gold.dim_listing_neighbourhood n
      ON  f.listing_id = n.listing_id
      AND f.month_year >= n.valid_from
      AND f.month_year <  n.valid_to
),
lga_rev AS (
    SELECT lga_code, AVG(estimated_revenue) AS avg_rev
    FROM listing_lga
    WHERE has_availability = 't'
    GROUP BY lga_code
),
joined AS (
    SELECT r.avg_rev, g2.median_age_persons AS median_age,
           g2.median_tot_hhd_inc_weekly AS hh_income,
           g2.average_household_size AS hh_size
    FROM lga_rev r
    JOIN gold.dim_census_g02 g2 ON r.lga_code = g2.lga_code
),
ranks AS (
    SELECT *,
           RANK() OVER (ORDER BY avg_rev)
             + (COUNT(*) OVER (PARTITION BY avg_rev) - 1) / 2.0     AS rank_rev,
           RANK() OVER (ORDER BY median_age)
             + (COUNT(*) OVER (PARTITION BY median_age) - 1) / 2.0  AS rank_age
    FROM joined
)
SELECT COUNT(*)                                                AS n_lgas,
       ROUND(CORR(median_age, avg_rev)::numeric, 3)            AS pearson_r_median_age,
       ROUND(REGR_R2(avg_rev, median_age)::numeric, 3)         AS r_squared_median_age,
       ROUND(CORR(rank_age, rank_rev)::numeric, 3)             AS spearman_rho_median_age,
       ROUND(CORR(hh_income, avg_rev)::numeric, 3)             AS pearson_r_household_income,
       ROUND(CORR(hh_size, avg_rev)::numeric, 3)               AS pearson_r_household_size
FROM ranks;

-- (b2) The data points behind the correlation (one row per LGA)
WITH listing_lga AS (
    SELECT f.has_availability, f.estimated_revenue,
           n.listing_neighbourhood AS lga_name, n.lga_code
    FROM gold.fact_listings f
    JOIN gold.dim_listing_neighbourhood n
      ON  f.listing_id = n.listing_id
      AND f.month_year >= n.valid_from
      AND f.month_year <  n.valid_to
)
SELECT l.lga_name,
       g2.median_age_persons                     AS median_age,
       ROUND(AVG(l.estimated_revenue), 0)        AS avg_revenue_per_active_listing
FROM listing_lga l
JOIN gold.dim_census_g02 g2 ON l.lga_code = g2.lga_code
WHERE l.has_availability = 't'
GROUP BY l.lga_name, g2.median_age_persons
ORDER BY median_age, avg_revenue_per_active_listing DESC;






/* =========================================================================
   (c) What will be the best type of listing (property type, room type and accommodates for) for the top 15 
       “listing_neighbourhood” (in terms of average estimated revenue per active listing) to have the highest 
       number of stays?
   ========================================================================= */

-- (c1) Best configuration per neighbourhood, ranked by TOTAL stays
--      Total stays captures overall demand for a configuration in the area.
WITH base AS (
    SELECT n.listing_neighbourhood, p.property_type, p.room_type, p.accommodates,
           f.listing_id, f.stays, f.estimated_revenue
    FROM gold.fact_listings f
    JOIN gold.dim_listing_neighbourhood n
      ON  f.listing_id = n.listing_id
      AND f.month_year >= n.valid_from
      AND f.month_year <  n.valid_to
    JOIN gold.dim_property p
      ON  f.listing_id = p.listing_id
      AND f.month_year >= p.valid_from
      AND f.month_year <  p.valid_to
    WHERE f.has_availability = 't'
),
top5 AS (
    SELECT listing_neighbourhood, AVG(estimated_revenue) AS avg_rev
    FROM base
    GROUP BY listing_neighbourhood
    ORDER BY avg_rev DESC
    LIMIT 5
),
configs AS (
    SELECT b.listing_neighbourhood, t.avg_rev,
           b.property_type, b.room_type, b.accommodates,
           SUM(b.stays)                  AS total_stays,
           ROUND(AVG(b.stays), 1)        AS avg_stays_per_listing_month,
           COUNT(DISTINCT b.listing_id)  AS distinct_listings
    FROM base b
    JOIN top5 t ON b.listing_neighbourhood = t.listing_neighbourhood
    GROUP BY b.listing_neighbourhood, t.avg_rev, b.property_type, b.room_type, b.accommodates
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY listing_neighbourhood
                              ORDER BY total_stays DESC) AS stays_rank
    FROM configs
)
SELECT listing_neighbourhood,
       ROUND(avg_rev, 0) AS nbhd_avg_revenue_per_active_listing,
       stays_rank, property_type, room_type, accommodates,
       total_stays, avg_stays_per_listing_month, distinct_listings
FROM ranked
WHERE stays_rank <= 3
ORDER BY avg_rev DESC, stays_rank;


-- (c2) Sensitivity check: best configuration by AVERAGE stays per
--      listing-month, only for configurations with at least 10 listings
--      (shows occupancy rather than volume).
WITH base AS (
    SELECT n.listing_neighbourhood, p.property_type, p.room_type, p.accommodates,
           f.listing_id, f.stays, f.estimated_revenue
    FROM gold.fact_listings f
    JOIN gold.dim_listing_neighbourhood n
      ON  f.listing_id = n.listing_id
      AND f.month_year >= n.valid_from
      AND f.month_year <  n.valid_to
    JOIN gold.dim_property p
      ON  f.listing_id = p.listing_id
      AND f.month_year >= p.valid_from
      AND f.month_year <  p.valid_to
    WHERE f.has_availability = 't'
),
top5 AS (
    SELECT listing_neighbourhood, AVG(estimated_revenue) AS avg_rev
    FROM base
    GROUP BY listing_neighbourhood
    ORDER BY avg_rev DESC
    LIMIT 5
),
configs AS (
    SELECT b.listing_neighbourhood, t.avg_rev,
           b.property_type, b.room_type, b.accommodates,
           ROUND(AVG(b.stays), 1)        AS avg_stays_per_listing_month,
           SUM(b.stays)                  AS total_stays,
           COUNT(DISTINCT b.listing_id)  AS distinct_listings
    FROM base b
    JOIN top5 t ON b.listing_neighbourhood = t.listing_neighbourhood
    GROUP BY b.listing_neighbourhood, t.avg_rev, b.property_type, b.room_type, b.accommodates
    HAVING COUNT(DISTINCT b.listing_id) >= 10
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY listing_neighbourhood
                              ORDER BY avg_stays_per_listing_month DESC) AS occupancy_rank
    FROM configs
)
SELECT listing_neighbourhood, property_type, room_type, accommodates,
       avg_stays_per_listing_month, total_stays, distinct_listings
FROM ranked
WHERE occupancy_rank = 1
ORDER BY avg_rev DESC;




/* =========================================================================
   (d) For hosts with multiple listings in Vic are their properties concentrated within the same LGA, or are 
       they distributed across different LGAs?
   ========================================================================= */

-- (d1) Overall answer and breakdown by portfolio size
WITH host_listings AS (
    SELECT f.host_id,
           COUNT(DISTINCT f.listing_id)            AS n_listings,
           COUNT(DISTINCT n.listing_neighbourhood) AS n_lgas
    FROM gold.fact_listings f
    JOIN gold.dim_listing_neighbourhood n
      ON  f.listing_id = n.listing_id
      AND f.month_year >= n.valid_from
      AND f.month_year <  n.valid_to
    GROUP BY f.host_id
    HAVING COUNT(DISTINCT f.listing_id) > 1
),
banded AS (
    SELECT *,
           CASE WHEN n_listings = 2  THEN '2 listings'
                WHEN n_listings <= 5 THEN '3-5 listings'
                WHEN n_listings <= 10 THEN '6-10 listings'
                ELSE '11+ listings' END AS portfolio_size,
           CASE WHEN n_listings = 2  THEN 1
                WHEN n_listings <= 5 THEN 2
                WHEN n_listings <= 10 THEN 3
                ELSE 4 END AS band_order
    FROM host_listings
)
SELECT portfolio_size,
       COUNT(*)                                                   AS hosts,
       SUM(CASE WHEN n_lgas = 1 THEN 1 ELSE 0 END)                AS hosts_in_one_lga,
       ROUND(100.0 * AVG(CASE WHEN n_lgas = 1 THEN 1 ELSE 0 END), 1) AS pct_in_one_lga,
       ROUND(AVG(n_lgas), 1)                                      AS avg_lgas_per_host
FROM banded
GROUP BY ROLLUP ((portfolio_size, band_order))
ORDER BY band_order NULLS LAST;


-- (d2) Distribution of multi-listing hosts by number of distinct LGAs
WITH host_listings AS (
    SELECT f.host_id,
           COUNT(DISTINCT f.listing_id)            AS n_listings,
           COUNT(DISTINCT n.listing_neighbourhood) AS n_lgas
    FROM gold.fact_listings f
    JOIN gold.dim_listing_neighbourhood n
      ON  f.listing_id = n.listing_id
      AND f.month_year >= n.valid_from
      AND f.month_year <  n.valid_to
    GROUP BY f.host_id
    HAVING COUNT(DISTINCT f.listing_id) > 1
)
SELECT n_lgas                                          AS distinct_lgas,
       COUNT(*)                                        AS hosts,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_multi_listing_hosts
FROM host_listings
GROUP BY n_lgas
ORDER BY n_lgas;




/* =========================================================================
   (e) For hosts with a single Airbnb listing in Vic does the estimated revenue over the last 12 months cover 
       the annualised median mortgage repayment in the corresponding LGA? Which LGA has the highest percentage 
       of hosts that can cover it?
       
       Annualised mortgage = G02 median_mortgage_repay_monthly * 12.
       The listing's LGA is its most recent (current) SCD2 version.
   ========================================================================= */

-- (e1) Overall share of single-listing hosts who can cover the mortgage
WITH single_hosts AS (
    SELECT host_id
    FROM gold.fact_listings
    GROUP BY host_id
    HAVING COUNT(DISTINCT listing_id) = 1
),
host_revenue AS (
    SELECT f.host_id, MAX(f.listing_id) AS listing_id,
           COALESCE(SUM(f.estimated_revenue), 0) AS revenue_12m
    FROM gold.fact_listings f
    JOIN single_hosts s ON f.host_id = s.host_id
    GROUP BY f.host_id
),
host_lga AS (
    SELECT h.host_id, h.revenue_12m, l.lga_code,
           g2.median_mortgage_repay_monthly * 12 AS annual_mortgage
    FROM host_revenue h
    JOIN gold.dim_listing_neighbourhood n
      ON  h.listing_id = n.listing_id
      AND n.valid_to = '9999-12-31'
    JOIN gold.dim_lga l        ON n.listing_neighbourhood = l.lga_name
    JOIN gold.dim_census_g02 g2 ON l.lga_code = g2.lga_code
)
SELECT COUNT(*)                                                           AS single_listing_hosts,
       SUM(CASE WHEN revenue_12m >= annual_mortgage THEN 1 ELSE 0 END)    AS hosts_covering_mortgage,
       ROUND(100.0 * AVG(CASE WHEN revenue_12m >= annual_mortgage
                              THEN 1 ELSE 0 END), 1)                      AS pct_covering_mortgage
FROM host_lga;

-- (e2) By LGA, highest share first (answers "which LGA is highest")
WITH single_hosts AS (
    SELECT host_id
    FROM gold.fact_listings
    GROUP BY host_id
    HAVING COUNT(DISTINCT listing_id) = 1
),
host_revenue AS (
    SELECT f.host_id, MAX(f.listing_id) AS listing_id,
           COALESCE(SUM(f.estimated_revenue), 0) AS revenue_12m
    FROM gold.fact_listings f
    JOIN single_hosts s ON f.host_id = s.host_id
    GROUP BY f.host_id
),
host_lga AS (
    SELECT h.host_id, h.revenue_12m, n.listing_neighbourhood AS lga_name,
           g2.median_mortgage_repay_monthly * 12 AS annual_mortgage
    FROM host_revenue h
    JOIN gold.dim_listing_neighbourhood n
      ON  h.listing_id = n.listing_id
      AND n.valid_to = '9999-12-31'
    JOIN gold.dim_lga l        ON n.listing_neighbourhood = l.lga_name
    JOIN gold.dim_census_g02 g2 ON l.lga_code = g2.lga_code
)
SELECT RANK() OVER (ORDER BY AVG(CASE WHEN revenue_12m >= annual_mortgage
                                      THEN 1.0 ELSE 0 END) DESC)          AS cover_rank,
       lga_name,
       COUNT(*)                                                           AS single_listing_hosts,
       SUM(CASE WHEN revenue_12m >= annual_mortgage THEN 1 ELSE 0 END)    AS hosts_covering,
       ROUND(100.0 * AVG(CASE WHEN revenue_12m >= annual_mortgage
                              THEN 1 ELSE 0 END), 1)                      AS pct_covering,
       MAX(annual_mortgage)                                               AS annual_median_mortgage,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue_12m)::numeric, 0)
                                                                          AS median_host_revenue_12m
FROM host_lga
GROUP BY lga_name
ORDER BY cover_rank, lga_name;
