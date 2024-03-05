import argparse
from pyspark.sql import SparkSession

def calculate_carrier_delay(data_source, output_uri):
    """
    Processes airline data and queries the data to find Year-wise carrier delays from 2003 to 2010.

    :param data_source: The URI of  airline data CSV
    :param output_uri: The URI where output is written
    """
    with SparkSession.builder.appName("Calculate Year-wise Carrier Delays").getOrCreate() as spark:
        # Load the airline data CSV
        if data_source is not None:
            airlines_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        airlines_df.createOrReplaceTempView("airline_data")

        # Create a DataFrame for Year-wise carrier delays from 2003 to 2010
        year_wise_carrier_delay = spark.sql("""
          SELECT Year, SUM(CarrierDelay) AS total_carrier_delay
          FROM airline_data
          WHERE Year BETWEEN 2003 AND 2010
          GROUP BY Year
          ORDER BY Year
        """)

        # Write the results to the specified output URI
        year_wise_carrier_delay.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for  CSV airline data")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved")
    args = parser.parse_args()

    calculate_carrier_delay(args.data_source, args.output_uri)
