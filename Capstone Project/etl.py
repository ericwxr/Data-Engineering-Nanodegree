from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, udf
from pyspark.sql.types import *
import datetime as dt


def create_spark_session():
    '''
        Description: initialize a spark session
        Returns:
            None
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport()\
        .getOrCreate()
    return spark


def process_airport_codes(spark, input_data, output_data):
    '''
        Description: This function is used to read airport data and write clean
            data to the dimensional table
        Arguments:
            spark: SparkSession initialized
            input_data: folder of the input data
            output_data: folder of the output data
        Returns:
            None
    '''
    df = pd.read_csv(input_data + "airport-codes_csv.csv")
    df = spark.read.csv(
        input_data + "airport-codes_csv.csv",
        header='true',
        inferSchema='true')
    
    # generate longitude and latitude from coordinates
    df = df\
        .withColumn("longitude", split(df.coordinates, ",")[0].cast("Double"))\
        .withColumn("latitude", split(df.coordinates, ",")[1].cast("Double"))\
        .drop("coordiantes")

    df.createOrReplaceTempView("airports")

    # extract columns to create songs table
    airports_table = spark.sql("""
        SELECT DISTINCT
            ident        AS ident,
            type         AS type,
            name         AS name,
            elevation_ft AS elevation_ft,
            continent    AS continent,
            iso_country  AS iso_country,
            iso_region   AS iso_region,
            municipality AS municipality,
            gps_code     AS gps_code,
            iata_code    AS iata_code,
            local_code   AS local_code,
            coordinates  AS coordinates
        FROM airports
        WHERE ident IS NOT NULL
    """)
    print("NOTE: {} records extracted to airports table.".format(airports_table.count()))

    airports_table.write.parquet(
        output_data + 'airports-tables/',
        mode="overwrite",
        partitionBy=("iso_country"))


def process_us_cities_demographics(spark, input_data, output_data):
    '''
        Description: This function is used to load the US cities demographics
            data and write clean data to the dimensional table
        Arguments:
            spark: SparkSession initialized
            input_data: folder of the input data
            output_data: folder of the output data
        Returns:
            None
    '''
    schema = StructType([
        StructField("city"                  ,StringType()),
        StructField("state"                 ,StringType()),
        StructField("median_age"            ,DoubleType()),
        StructField("male_population"       ,StringType()),
        StructField("female_population"     ,StringType()),
        StructField("total_population"      ,IntegerType()),
        StructField("number_of_veterans"    ,IntegerType()),
        StructField("number_of_foreign_born",IntegerType()),
        StructField("average_household_size",DoubleType()),
        StructField("state_code"            ,StringType()),
        StructField("race"                  ,StringType()),
        StructField("count"                 ,IntegerType()) 
    ])

    df = spark.read.csv(
        input_data + 'us-cities-demographics.csv',
        header='true',
        sep=";",
        schema=schema)
    print("NOTE: {} records extracted to demographics table.".format(df.count()))

    df.write.parquet(
        output_data + 'demographics-tables/',
        mode="overwrite",
        partitionBy=("state_code"))


def process_other_dimensional(spark, input_data, output_data, dimensions):
    '''
        Description: This function is used to load other dimensional tables
            exracted from SAS description
        Arguments:
            spark: SparkSession initialized
            input_data: folder of the input data
            output_data: folder of the output data
        Returns:
            None
    '''

    for dimension in dimensions:
        df = spark.read.csv(
            input_data + dimension + '.csv',
            header='true',
            inferSchema='true')
        print("NOTE: {} records extracted to {} table.".format(df.count(), dimension))

        df.write.parquet(
            output_data + dimension + '-tables/',
            mode="overwrite")


def process_arrival_data(spark, input_data, output_data, dimensions):
    '''
        Description: This function is used to extract I-94 arrival data,
            create dimensional tables, and write parquet files to the
            output folder
        Arguments:
            spark: SparkSession initialized
            input_data: folder of the input data
            output_data: folder of the output data
        Returns:
            None
    '''
    # Read arrival data file
    df = spark.read\
        .format('com.github.saurfang.sas.spark')\
        .load(input_data + 'i94_apr16_sub.sas7bdat')

    # filter arrival data by looking at valid dimension values
    count = df.count()
    for dimension in dimensions:
        valid_list = spark.read.parquet(
            output_data + dimension + '-tables/').select(dimension).rdd.flatMap(lambda x: x).collect()

        if dimension == 'i94cit_i94res':
            df = df.filter(df['i94cit'].isin(valid_list) & df['i94res'].isin(valid_list))
        else:
            df = df.filter(df[dimension].isin(valid_list))

        diff = count - df.count()
        print(
            "NOTE: {} records were removed by {} validation check"
            .format(diff, dimension))

    # SAS stores date in the format of time difference since 1Jan1960, converting as below
    get_date = udf(lambda x: dt.date(1960, 1, 1) + dt.timedelta(days=x) if x else None)
    df = df.withColumn("arrdate", get_date(df.arrdate))\
        .withColumn("depdate", get_date(df.depdate))

    df.createOrReplaceTempView("staging_records")

    # extract columns from staging table to generate admissions table
    # the admission number of each valid travel document won't change
    # no matter how many times one has been to the US
    admissions_table = spark.sql("""
        SELECT DISTINCT
            admnum,
            biryear,
            gender
        FROM staging_records
        WHERE admnum IS NOT NULL
    """)
    print("NOTE: {} records extracted to admissions table.".format(admissions_table.count()))

    # write table to parquet files partitioned by biryear
    admissions_table.write.parquet(
        output_data + 'admissions-tables',
        mode="overwrite",
        partitionBy=("biryear"))

    # extract columns from staging table create fact table
    arrivals_table = spark.sql("""
        SELECT DISTINCT
            i94yr,
            i94mon,
            i94cit,
            i94res,
            i94port,
            arrdate,
            i94mode,
            i94addr,
            depdate,
            i94bir,
            i94visa,
            count,
            dtadfile,
            visapost,
            occup,
            entdepa,
            entdepd,
            entdepu,
            matflag,
            dtaddto,
            insnum,
            airline,
            admnum,
            fltno,
            visatype
        FROM staging_records
    """)
    print("NOTE: {} records extracted to arrivals table.".format(admissions_table.count()))

    # write table to parquet files partitioned by year and month
    arrivals_table.write.parquet(
        output_data + 'arrivals-tables',
        mode="overwrite",
        partitionBy=("i94yr", "i94mon"))


def main():
    spark = create_spark_session()
    input_data = "Data/"
    output_data = "Data/Output/"

    process_airport_codes(spark, input_data, output_data)
    process_us_cities_demographics(spark, input_data, output_data)

    dimensions = ['i94port', 'i94visa', 'i94addr', 'i94mode', 'i94cit_i94res']
    process_other_dimensional(spark, input_data, output_data, dimensions)

    dimension_filters = ['i94port', 'i94visa']
    process_arrival_data(spark, input_data, output_data, dimension_filters)

    print("NOTE: Process done.")

if __name__ == "__main__":
    main()
