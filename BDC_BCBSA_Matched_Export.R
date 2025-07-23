
# --- BDC BCBSA-Aligned Counts Script ---

library(tidyverse)
library(vroom)
library(readxl)
library(RODBC)

# --- Paths ---
bdc_path <- "C:/BDC/"
blank_shell <- "C:/BDC/blank_.accdb"
access_target <- file.path(bdc_path, paste0("BDC_BCBSA_Matched_", Sys.Date(), ".accdb"))

# --- Load BDC Files ---
bdc_files <- list.files(bdc_path, pattern = "\.txt\.gz$", full.names = TRUE)

BDC_Data <- map_dfr(bdc_files, function(file) {
  vroom(file, delim = "|", col_types = cols(.default = "c")) %>%
    rename(PROV_MSTR_LOC_ID = BCBSA_PROV_MSTR_LOC_ID) %>%
    mutate(across(where(is.character), str_to_upper))
})

# --- Standardize Program Descriptions ---
BDC_Cleaned <- BDC_Data %>%
  mutate(
    Program_Category = case_when(
      str_detect(Bdc_Program_DESCRIPTION, "BARIATRIC") ~ "BARIATRIC SURGERY",
      str_detect(Bdc_Program_DESCRIPTION, "CARDIAC") ~ "CARDIAC CARE",
      str_detect(Bdc_Program_DESCRIPTION, "CANCER") ~ "CANCER CARE",
      str_detect(Bdc_Program_DESCRIPTION, "CELLULAR IMMUNOTHERAPY") ~ "CELLULAR IMMUNOTHERAPY",
      str_detect(Bdc_Program_DESCRIPTION, "FERTILITY") ~ "FERTILITY CARE",
      str_detect(Bdc_Program_DESCRIPTION, "GENE") ~ "GENE THERAPY",
      str_detect(Bdc_Program_DESCRIPTION, "KNEE AND HIP") ~ "KNEE AND HIP REPLACEMENT",
      str_detect(Bdc_Program_DESCRIPTION, "MATERNITY") ~ "MATERNITY CARE",
      str_detect(Bdc_Program_DESCRIPTION, "SPINE") ~ "SPINE SURGERY",
      str_detect(Bdc_Program_DESCRIPTION, "SUBSTANCE USE") ~ "SUBSTANCE USE TREATMENT AND RECOVERY",
      str_detect(Bdc_Program_DESCRIPTION, "TRANSPLANTS") ~ "TRANSPLANTS",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(Program_Category)) %>%
  filter(RECOGNTN_ENTITY_CD %in% c("P", "F"))  # Filter for facility and professional only

# --- Deduplicate using NPI + ZIP + Designation + Program ---
BDC_Cleaned_Dedup <- BDC_Cleaned %>%
  distinct(NPI, PROV_ADRS_ZIP_CD, Designation_TYPE, Program_Category, .keep_all = TRUE)

# --- Summary Count (optional) ---
count_summary <- BDC_Cleaned_Dedup %>%
  count(Program_Category, Designation_TYPE) %>%
  arrange(Program_Category, Designation_TYPE)

print(count_summary)

# --- Prepare for Access Export ---
BDC_Export <- BDC_Cleaned_Dedup %>%
  mutate(
    PROVIDER_NAME = ORG_NAME,
    NTWK = "",
    PROVTYPE = "",
    TAXONOMY = TXNMY_CD,
    RowId = row_number(),
    Latitude = NA_character_,
    Longitude = NA_character_,
    GeoInfo = NA_character_,
    StandardZip = NA_character_,
    StandardStat = NA_character_,
    CountySSA = NA_character_
  ) %>%
  transmute(
    NTWK,
    PROVTYPE,
    NPI = PROV_NPI,
    PROVIDER_NAME,
    ADDRESS = PROV_ADDR_LINE_1,
    CITY = PROV_ADRS_CITY,
    STATE = PROV_ADRS_ST,
    ZIP_5 = PROV_ADRS_ZIP_CD,
    TAXONOMY,
    RowId,
    Latitude,
    Longitude,
    GeoInfo,
    StandardZip,
    StandardStat,
    CountySSA,
    Bdc_Program_DESCRIPTION,
    RECOGNTN_ENTITY_CD,
    Recogntn_Value_CD,
    Designation_TYPE,
    Program_Category
  )

# --- Export to Access ---
file.copy(blank_shell, access_target, overwrite = TRUE)

conn <- odbcDriverConnect(paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=", access_target))

chunk_size <- 10000
n <- nrow(BDC_Export)

for (start in seq(1, n, by = chunk_size)) {
  end <- min(start + chunk_size - 1, n)
  chunk <- BDC_Export[start:end, ]
  sqlSave(conn, chunk, tablename = "NTWK", rownames = FALSE, append = (start != 1), safer = TRUE)
}

odbcClose(conn)
message("✅ Export complete: ", access_target)
