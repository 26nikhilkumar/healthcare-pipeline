from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, to_date, when, datediff
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Initialize Spark session with optimized configurations
spark = SparkSession.builder \
    .appName("HealthcarePipeline") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

def clean_datasets(patients, symptoms, medications, conditions, encounters, patient_gender):
    # Define schemas
    patients_schema = StructType([
        StructField("PATIENT_ID", StringType(), True),
        StructField("BIRTHDATE", StringType(), True),
        StructField("DEATHDATE", StringType(), True),
        StructField("SSN", StringType(), True),
        StructField("DRIVERS", StringType(), True),
        StructField("PASSPORT", StringType(), True),
        StructField("PREFIX", StringType(), True),
        StructField("FIRST", StringType(), True),
        StructField("LAST", StringType(), True),
        StructField("SUFFIX", StringType(), True),
        StructField("MAIDEN", StringType(), True),
        StructField("MARITAL", StringType(), True),
        StructField("RACE", StringType(), True),
        StructField("ETHNICITY", StringType(), True),
        StructField("GENDER", StringType(), True),
        StructField("BIRTHPLACE", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTY", StringType(), True),
        StructField("FIPS", StringType(), True),
        StructField("ZIP", StringType(), True),
        StructField("LAT", StringType(), True),
        StructField("LON", StringType(), True),
        StructField("HEALTHCARE_EXPENSES", DoubleType(), True),
        StructField("HEALTHCARE_COVERAGE", DoubleType(), True),
        StructField("INCOME", DoubleType(), True)
    ])

    encounters_schema = StructType([
        StructField("Id", StringType(), True),
        StructField("START", StringType(), True),
        StructField("STOP", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ORGANIZATION", StringType(), True),
        StructField("PROVIDER", StringType(), True),
        StructField("PAYER", StringType(), True),
        StructField("ENCOUNTERCLASS", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("BASE_ENCOUNTER_COST", DoubleType(), True),
        StructField("TOTAL_CLAIM_COST", DoubleType(), True),
        StructField("PAYER_COVERAGE", DoubleType(), True),
        StructField("REASONCODE", StringType(), True),
        StructField("REASONDESCRIPTION", StringType(), True),
        StructField("DURATION", IntegerType(), True)
    ])

    symptoms_schema = StructType([
        StructField("PATIENT", StringType(), True),
        StructField("GENDER", StringType(), True),
        StructField("RACE", StringType(), True),
        StructField("ETHNICITY", StringType(), True),
        StructField("AGE_BEGIN", DoubleType(), True),
        StructField("AGE_END", DoubleType(), True),
        StructField("PATHOLOGY", StringType(), True),
        StructField("NUM_SYMPTOMS", IntegerType(), True),
        StructField("SYMPTOMS", StringType(), True)
    ])

    medications_schema = StructType([
        StructField("START", StringType(), True),
        StructField("STOP", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("PAYER", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("BASE_COST", DoubleType(), True),
        StructField("PAYER_COVERAGE", DoubleType(), True),
        StructField("DISPENSES", IntegerType(), True),
        StructField("TOTALCOST", DoubleType(), True),
        StructField("REASONCODE", StringType(), True),
        StructField("REASONDESCRIPTION", StringType(), True)
    ])

    conditions_schema = StructType([
        StructField("START", StringType(), True),
        StructField("STOP", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("DURATION", IntegerType(), True)
    ])

    patient_gender_schema = StructType([
        StructField("Id", StringType(), True),
        StructField("GENDER", StringType(), True)
    ])

    # Convert Pandas DataFrames to Spark DataFrames
    patients = patients.astype({"INCOME": float})
    symptoms = symptoms.astype({"AGE_BEGIN": float, "AGE_END": float})
    patients_df = spark.createDataFrame(patients.to_dict(orient='records'), schema=patients_schema)
    symptoms_df = spark.createDataFrame(symptoms.to_dict(orient='records'), schema=symptoms_schema)
    medications_df = spark.createDataFrame(medications.to_dict(orient='records'), schema=medications_schema)
    conditions_df = spark.createDataFrame(conditions.to_dict(orient='records'), schema=conditions_schema)
    encounters_df = spark.createDataFrame(encounters.to_dict(orient='records'), schema=encounters_schema)
    patient_gender_df = spark.createDataFrame(patient_gender.to_dict(orient='records'), schema=patient_gender_schema)

    # Persist DataFrames to avoid recomputation
    patients_df.persist()
    symptoms_df.persist()
    medications_df.persist()
    conditions_df.persist()
    encounters_df.persist()
    patient_gender_df.persist()

    # Clean patients
    patients_df = patients_df.withColumn("birth_date", to_date(col("BIRTHDATE"), 'yyyy-MM-dd')) \
                             .withColumn("death_date", when(col("DEATHDATE") == "", None).otherwise(to_date(col("DEATHDATE"), 'yyyy-MM-dd'))) \
                             .fillna({'GENDER': 'Unknown'})

    # Clean symptoms
    symptoms_df = symptoms_df.withColumn("AGE_BEGIN", col("AGE_BEGIN").cast(IntegerType())) \
                             .withColumn("AGE_END", col("AGE_END").cast(IntegerType()))
    symptoms_split = symptoms_df.withColumn("SYMPTOMS", split(col("SYMPTOMS"), ";"))
    symptoms_exploded = symptoms_split.withColumn("SYMPTOMS", explode("SYMPTOMS"))
    symptoms_extracted = symptoms_exploded.withColumn("symptom", split(col("SYMPTOMS"), ":").getItem(0)) \
                                          .withColumn("frequency", split(col("SYMPTOMS"), ":").getItem(1))
    symptoms_cleaned = symptoms_extracted.drop("SYMPTOMS")

    # Clean medications
    medications_df = medications_df.withColumn("start", to_date(col("START"), 'yyyy-MM-dd')) \
                                   .withColumn("stop", to_date(col("STOP"), 'yyyy-MM-dd'))
    medications_df = medications_df.withColumn("out_of_pocket", col("TOTALCOST") - col("PAYER_COVERAGE"))

    # Clean conditions
    conditions_df = conditions_df.withColumn("start", to_date(col("START"), 'yyyy-MM-dd')) \
                                 .withColumn("stop", to_date(col("STOP"), 'yyyy-MM-dd'))
    conditions_df = conditions_df.withColumn("duration", datediff(col("stop"), col("start")))

    # Clean encounters
    encounters_df = encounters_df.withColumn("start", to_date(col("START"), 'yyyy-MM-dd')) \
                                 .withColumn("stop", to_date(col("STOP"), 'yyyy-MM-dd'))
    encounters_df = encounters_df.withColumn("duration", datediff(col("stop"), col("start")))
    encounters_df = encounters_df.fillna({'REASONDESCRIPTION': 'None'})

    return patients_df, symptoms_cleaned, medications_df, conditions_df, encounters_df, patient_gender_df
