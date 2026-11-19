-- (a) dm_listing_neighbourhood: one neighbourhood across all 12 months
SELECT * FROM datamart.dm_listing_neighbourhood
WHERE listing_neighbourhood = 'MOSMAN'
ORDER BY month_year;

-- (b) dm_property_type: one listing type across the months
SELECT * FROM datamart.dm_property_type
WHERE property_type = 'Entire apartment'
  AND room_type = 'Entire home/apt'
  AND accommodates = 2
ORDER BY month_year;

-- (c) dm_host_neighbourhood: one host LGA across all 12 months
SELECT * FROM datamart.dm_host_neighbourhood
WHERE host_neighbourhood_lga = 'SYDNEY'
ORDER BY month_year;