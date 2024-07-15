# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

import pyspark.sql.functions as f
volumepath = "/Volumes/mimi_ws_1/desynpuf/src/unzipped/"
catalog = "mimi_ws_1"
schema = "desynpuf"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Beneficiary Summary

# COMMAND ----------

benefile_pattern = "DE1_0_*_Beneficiary_Summary_File_Sample_*.csv"
tabname = "beneficiary_summary"
for file in Path(volumepath).glob(benefile_pattern):
    year = file.name.split("_")[2]
    mimi_src_file_date = parse(f"{year}-01-01").date()
    df = (spark.read.format("csv")
                .option("sep", ",")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("header", "true")
                .load(str(file)))
    header = []
    for col_old, col_new in zip(df.columns, change_header(df.columns)):    
        header.append(col_new)
        if col_new in {'bene_birth_dt', 'bene_death_dt'}:
            df = df.withColumn(col_new, f.to_date(f.col(col_old), "yyyyMMdd"))
        elif col_new in {'bene_hi_cvrage_tot_mons',
                            'bene_smi_cvrage_tot_mons',
                            'bene_hmo_cvrage_tot_mons',
                            'plan_cvrg_mos_num'}:
            df = df.withColumn(col_new, f.col(col_old).cast('int'))
        elif col_new in {'medreimb_ip',
                            'benres_ip',
                            'pppymt_ip',
                            'medreimb_op',
                            'benres_op',
                            'pppymt_op',
                            'medreimb_car',
                            'benres_car',
                            'pppymt_car'}:
            df = df.withColumn(col_new, f.col(col_old).cast('double'))
        else:
            df = df.withColumnRenamed(col_old, col_new)
    
    df = (df.select(*header)
            .withColumn("mimi_src_file_date", f.lit(mimi_src_file_date))
            .withColumn("mimi_src_file_name", f.lit(file.name))
            .withColumn("mimi_dlt_load_date", f.lit(datetime.today().date())))

    (df.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_name = '{file.name}'")
        .saveAsTable(f"{catalog}.{schema}.{tabname}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carrier Claims

# COMMAND ----------

carrier_claims_pattern = "DE1_0_2008_to_2010_Carrier_Claims_Sample_*.csv"
tabname = "carrier_claims"
for file in Path(volumepath).glob(carrier_claims_pattern):
    mimi_src_file_date = parse("2008-01-01").date()
    df = (spark.read.format("csv")
                .option("sep", ",")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("header", "true")
                .load(str(file)))
    header = []
    for col_old, col_new in zip(df.columns, change_header(df.columns)):    
        header.append(col_new)
        if col_new in {'clm_from_dt', 'clm_thru_dt'}:
            df = df.withColumn(col_new, f.to_date(f.col(col_old), "yyyyMMdd"))
        elif '_amt_' in col_new:
            df = df.withColumn(col_new, f.col(col_old).cast('double'))
        else:
            df = df.withColumnRenamed(col_old, col_new)
    
    df = (df.select(*header)
            .withColumn("mimi_src_file_date", f.lit(mimi_src_file_date))
            .withColumn("mimi_src_file_name", f.lit(file.name))
            .withColumn("mimi_dlt_load_date", f.lit(datetime.today().date())))

    (df.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_name = '{file.name}'")
        .saveAsTable(f"{catalog}.{schema}.{tabname}"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Inpatient Claims

# COMMAND ----------


inpatient_claims_pattern = "DE1_0_2008_to_2010_Inpatient_Claims_Sample_*.csv"
tabname = "inpatient_claims"
for file in Path(volumepath).glob(inpatient_claims_pattern):
    mimi_src_file_date = parse("2008-01-01").date()
    df = (spark.read.format("csv")
                .option("sep", ",")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("header", "true")
                .load(str(file)))
    header = []
    for col_old, col_new in zip(df.columns, change_header(df.columns)):    
        header.append(col_new)
        if col_new[-3:] == '_dt':
            df = df.withColumn(col_new, f.to_date(f.col(col_old), "yyyyMMdd"))
        elif col_new[-4:] == '_cnt':
            df = df.withColumn(col_new, f.col(col_old).cast('int'))
        elif col_new[-4:] == '_amt' or col_new[-3:] == '_am':
            df = df.withColumn(col_new, f.col(col_old).cast('double'))
        else:
            df = df.withColumnRenamed(col_old, col_new)
    
    df = (df.select(*header)
            .withColumn("mimi_src_file_date", f.lit(mimi_src_file_date))
            .withColumn("mimi_src_file_name", f.lit(file.name))
            .withColumn("mimi_dlt_load_date", f.lit(datetime.today().date())))

    (df.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_name = '{file.name}'")
        .saveAsTable(f"{catalog}.{schema}.{tabname}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outpatient Claims

# COMMAND ----------

outpatient_claims_pattern = "DE1_0_2008_to_2010_Outpatient_Claims_Sample_*.csv"
tabname = "outpatient_claims"
for file in Path(volumepath).glob(outpatient_claims_pattern):
    mimi_src_file_date = parse("2008-01-01").date()
    df = (spark.read.format("csv")
                .option("sep", ",")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("header", "true")
                .load(str(file)))
    header = []
    for col_old, col_new in zip(df.columns, change_header(df.columns)):    
        header.append(col_new)
        if col_new[-3:] == '_dt':
            df = df.withColumn(col_new, f.to_date(f.col(col_old), "yyyyMMdd"))
        elif col_new[-4:] == '_amt' or col_new[-3:] == '_am':
            df = df.withColumn(col_new, f.col(col_old).cast('double'))
        else:
            df = df.withColumnRenamed(col_old, col_new)
    
    df = (df.select(*header)
            .withColumn("mimi_src_file_date", f.lit(mimi_src_file_date))
            .withColumn("mimi_src_file_name", f.lit(file.name))
            .withColumn("mimi_dlt_load_date", f.lit(datetime.today().date())))

    (df.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_name = '{file.name}'")
        .saveAsTable(f"{catalog}.{schema}.{tabname}"))    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prescription Drug Events

# COMMAND ----------

prescription_drug_pattern = "DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_*.csv"
tabname = "prescription_drug_events"
for file in Path(volumepath).glob(prescription_drug_pattern):
    mimi_src_file_date = parse("2008-01-01").date()
    df = (spark.read.format("csv")
                .option("sep", ",")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("header", "true")
                .load(str(file)))
    header = []
    for col_old, col_new in zip(df.columns, change_header(df.columns)):    
        header.append(col_new)
        if col_new[-3:] == '_dt':
            df = df.withColumn(col_new, f.to_date(f.col(col_old), "yyyyMMdd"))
        elif col_new[-4:] == '_amt' or col_new[-4:] == '_num':
            df = df.withColumn(col_new, f.col(col_old).cast('double'))
        else:
            df = df.withColumnRenamed(col_old, col_new)
    
    df = (df.select(*header)
            .withColumn("mimi_src_file_date", f.lit(mimi_src_file_date))
            .withColumn("mimi_src_file_name", f.lit(file.name))
            .withColumn("mimi_dlt_load_date", f.lit(datetime.today().date())))

    (df.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_name = '{file.name}'")
        .saveAsTable(f"{catalog}.{schema}.{tabname}"))


# COMMAND ----------


