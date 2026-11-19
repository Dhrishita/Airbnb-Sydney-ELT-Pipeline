SELECT source_file,
       COUNT(*)       AS row_count,
       MIN(loaded_at) AS loaded_at
FROM bronze.raw_listings
GROUP BY source_file
ORDER BY RIGHT(source_file, 8), source_file;