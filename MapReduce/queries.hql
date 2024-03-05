SELECT Year, AVG((CarrierDelay/ArrDelay)*100) AS Year_wise_carrier_delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;

SELECT Year, AVG((NASDelay/ArrDelay)*100) AS Year_wise_carrier_delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;

SELECT Year, AVG((WeatherDelay/ArrDelay)*100) AS Year_wise_carrier_delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;

SELECT Year, AVG((LateAircraftDelay/ArrDelay)*100) AS Year_wise_carrier_delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;

SELECT Year, AVG((SecurityDelay/ArrDelay)*100) AS Year_wise_carrier_delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;

