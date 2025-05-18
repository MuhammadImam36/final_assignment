# Import Libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import regexp_replace, col, when, month, dayofmonth, dayofweek, row_number, dense_rank
from pyspark.sql.window import Window
import os

# Load PostgreSQL connection details from environment variables
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dwh = os.getenv('POSTGRES_DWH')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

# Connection Setup between Spark and Data Warehouse
jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_dwh}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver'
}

# ==============================================================================
#                             Variable Configuration
# ==============================================================================
harga_harian_directory = "/opt/airflow/data/data_harga_harian.csv"
harga_harian_schema = StructType([
    StructField("date", DateType(), True),
    StructField("id_provinsi", IntegerType(), True),
    StructField("id_komoditas", IntegerType(), True),
    StructField("harga_rata_rata", IntegerType(), True),
    StructField("het", StringType(), True),
    StructField("disparitas_het", FloatType(), True),
    StructField("status", StringType(), True)
])

# Provinsi
provinsi_directory = "/opt/airflow/data/metadata_provinsi.csv"
provinsi_schema = schema = StructType([
    StructField("id_provinsi", IntegerType(), True),
    StructField("nama_provinsi", StringType(), True),
    StructField("is_produsen", StringType(), True)
])

# Komoditas
komoditas_directory = "/opt/airflow/data/metadata_komoditas.csv"
komoditas_schema = schema = StructType([
    StructField("id_komoditas", IntegerType(), True),
    StructField("komoditas", StringType(), True),
    StructField("nama_sektor", StringType(), True)
])

# Status
status_directory = "/opt/airflow/data/metadata_status.csv"
status_schema = schema = StructType([
    StructField("deskripsi_status", StringType(), True),
])

# fact_harga_harian config
config = {
    'harga_harian': {'schema': harga_harian_schema, 'directory': harga_harian_directory},
    'dim_status': {'schema': status_schema, 'directory': status_directory}
}

# ==============================================================================
#                                     Module
# ==============================================================================
class BPNTransformer:
    def __init__(self):
        self.print = 'Ini adalah modul untuk melakukan transformasi data hasil scrape pada website BPN'

    def data_harga_harian(self, schema, directory):
        harga_harian_df = spark.read.csv(directory, header=True, schema = schema)
        harga_harian_df = (harga_harian_df
                .withColumn("het", regexp_replace("het", "\.", "")) 
                .withColumn("het", col("het").cast("int"))
                .withColumn("status",
                    when(col("status") == "green", "Aman")
                    .when(col("status") == "yellow", "Waspada")
                    .when(col("status") == "red", "Intervensi")
                    .otherwise("tidak diketahui")))
        return harga_harian_df

    def dim_date(self, schema, directory):
        date_df = self.data_harga_harian(schema, directory)
        date_df = date_df.select("date").dropDuplicates()
        date_df = date_df.withColumn("month", month("date")) \
                        .withColumn("day_of_month", dayofmonth("date")) \
                        .withColumn("day_of_week", dayofweek("date"))
        window_spec = Window.orderBy("date")
        date_df = date_df.withColumn("id_date", dense_rank().over(window_spec))

        return date_df.select("id_date", "date", "month", "day_of_month", "day_of_week")

    def dim_komoditas(self, schema, directory):
        komoditas_df = spark.read.csv(directory, header=True, schema = schema)
        return komoditas_df

    def dim_provinsi(self, schema, directory):
        provinsi_df = spark.read.csv(directory, header=True, schema=schema)
        provinsi_df_2 = spark.createDataFrame([
            (1006, "Nasional", "None"),
            (1007, "Zona 1", "None"),
            (1008, "Zona 2", "None"),
            (1009, "Zona 3", "None"),
        ], ["id_provinsi", "nama_provinsi", "is_produsen"])
        provinsi_df_merged = provinsi_df.union(provinsi_df_2)
        provinsi_df_merged = provinsi_df_merged.withColumn(
            "is_provinsi",
            when(
                (col("nama_provinsi") == 'Nasional') |
                (col("nama_provinsi") == 'Zona 1') |
                (col("nama_provinsi") == 'Zona 2') |
                (col("nama_provinsi") == 'Zona 3'),
                'n'
            ).otherwise('y'))
        return provinsi_df_merged

    def dim_status(self, schema, directory):
        windowSpec = Window.orderBy("deskripsi_status")
        status_df = spark.read.csv(directory, header=True, schema = schema)
        status_df = status_df.withColumn("id_status", row_number().over(windowSpec))
        status_df_2 = spark.createDataFrame([
                    (1, "Aman"),
                    (2, "Waspada"),
                    (3, "Intervensi")
                ], ["id_status", "status"])
        status_df_merged = status_df.join(status_df_2, on="id_status", how="inner")
        return status_df_merged

    def fact_harga_harian(self, **kwargs):
        harga_harian_params = kwargs['harga_harian']
        dim_status_params = kwargs['dim_status']
        harga_harian_df = (self.data_harga_harian(**harga_harian_params)
                .join(self.dim_date(**harga_harian_params).select('id_date', 'date'), on='date', how='left').drop('date')
                .join(self.dim_status(**dim_status_params).select('id_status', 'status'), on='status', how='left').drop('status'))
        return harga_harian_df

# Helper Function
def create_spark_session(app_name: str, log_level: str, partitions: str ) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", partitions) \
        .getOrCreate()

    spark.sparkContext.setLogLevel(log_level)
    
    return spark

# Instantiate SparkSession
spark = create_spark_session("Final_Assignment", "WARN", "8")
# df = spark.read.csv("/opt/airflow/data/data_harian.csv", header=True, inferSchema=True)
# df.show(10)
# df.printSchema()
transformer = BPNTransformer()
harga_harian = transformer.data_harga_harian(harga_harian_schema, harga_harian_directory)
dim_date = transformer.dim_date(harga_harian_schema, harga_harian_directory)
dim_komoditas = transformer.dim_komoditas(komoditas_schema, komoditas_directory)
dim_provinsi = transformer.dim_provinsi(provinsi_schema, provinsi_directory)
dim_status = transformer.dim_status(status_schema, status_directory)
fact_harga_harian = transformer.fact_harga_harian(**config)

tables = [
    (dim_date, "dim_date"),
    (dim_komoditas, "dim_komoditas"),
    (dim_provinsi, "dim_provinsi"),
    (dim_status, "dim_status"),
    (fact_harga_harian, "fact_harga_harian")
]

try:
    for df, table_name in tables:
        df.write.jdbc(
            url=jdbc_url,
            table=f'public.{table_name}',
            mode="append",
            properties=jdbc_properties
        )
except Exception as e:
    print("ERROR during Spark job:", str(e))
    raise e