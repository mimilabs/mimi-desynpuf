# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/download_utils

# COMMAND ----------

urls = []
for i in range(1, 21):
    urls += [f"https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2008_beneficiary_summary_file_sample_{i}.zip",
    f"http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_{i}A.zip",
    f"http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_{i}B.zip",
    f"https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2008_to_2010_inpatient_claims_sample_{i}.zip",
    f"https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2008_to_2010_outpatient_claims_sample_{i}.zip",
    f"http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_{i}.zip",
    f"https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2009_beneficiary_summary_file_sample_{i}.zip",
    f"https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2010_beneficiary_summary_file_sample_{i}.zip"
    ]

# COMMAND ----------

exceptions = {
    "https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2010_beneficiary_summary_file_sample_1.zip": "https://www.cms.gov/sites/default/files/2020-09/DE1_0_2010_Beneficiary_Summary_File_Sample_1.zip"
}

# COMMAND ----------

urls = [exceptions.get(url, url) for url in urls]

# COMMAND ----------

download_files(urls, "/Volumes/mimi_ws_1/desynpuf/src/zipfiles")

# COMMAND ----------

errors = ["DE1_0_2008_to_2010_Carrier_Claims_Sample_11A.zip"] # need to unzip manually

# COMMAND ----------

for path_zip in Path("/Volumes/mimi_ws_1/desynpuf/src/zipfiles").glob("*"):
    if path_zip.name in errors:
        continue
    unzip(path_zip, "/Volumes/mimi_ws_1/desynpuf/src/unzipped")

# COMMAND ----------


