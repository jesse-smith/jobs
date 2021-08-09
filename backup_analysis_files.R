library(magrittr)
library(dplyr)
library(fs)
options(rlang_backtrace_on_error = "branch", error = rlang::entrace)

# Macro Variables - Edit These #################################################

# Base path for all files
base_path <- "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/"

# Path extension to archive folder
archive_folder <- "COVID Data Analysis Archive/archive_7day/"

# SAS file name
sas_file <- paste0(
  "covid19_sandbox_dataanalysis_sascode_excl ",
  "ael_merge_11_30_14daymodified_use.sas"
)

# Data analysis 1 name
da1_file <- "COVID19_Data Analysis1_5-3-20_1.xlsx"

# Data analysis 2 name
da2_file <- "COVID19_Data Analysis2_5-3-20.xlsx"

# 14-Day trends name
trends14_file <- "COVID19_Data Analysis_14daytrend-linked (5.15.2020).xlsx"

# Zipcode name
zipcode_file <- "Confirmed COVID-19 SC cases 03312020_10AM_Zipcode.xlsx"

# Overwrite existing files?
overwrite = TRUE

# Script Code - DO NOT EDIT ####################################################

run_script <- function() {

  error_notify <- function(error, .action = "saving", .file) {
    coviData::notify(
      to = "Jesse.Smith@shelbycountytn.gov",
      subject = paste0(.file, " Backup Failed on ", Sys.Date()),
      body = paste0(
        "The 'backup_analysis_files.R' script failed while ",
        .action, " ", .file, " with the following error:\n\n",
        error$message, "\n\n",
        "See log for details."
      )
    )
  }

  # coviData already has a function for creating paths
  create_path <- tryCatch(
    coviData::create_path,
    error = function(error) {
      error_notify(error, .action = "loading", .file = "`create_path()`")

      rlang::cnd_signal(error)
    }
  )

  message("\n", Sys.time())

  message("Creating archive path...")
  archive_path <- tryCatch(
    create_path(base_path, archive_folder),
    error = function(error) {
      error_notify(error, .action = "creating", .file = "`archive_path`")

      rlang::cnd_signal(error)
    }
  )

  message("Copying files to backup folder...")

  message("SAS file...")
  tryCatch(
    file_copy(
      path = create_path(base_path, sas_file),
      new_path = create_path(
        archive_path,
        paste0("sas_prgm_backup_", Sys.Date(), ".sas")
      ),
      overwrite = overwrite
    ),

    error = function(error) {

      error_notify(error = error, .file = "SAS Program")

      try(rlang::cnd_signal(error))

    }
  )

  message("Data Analysis 1...")
  # Copy Data Analysis 1
  tryCatch(
    file_copy(
      path = create_path(base_path, da1_file),
      new_path = create_path(
        archive_path,
        paste0("data_analysis_1_backup", Sys.Date(), ".xlsx")
      ),
      overwrite = overwrite
    ),
    error = function(error) {
      error_notify(error = error, .file = "")

      try(rlang::cnd_signal(error))
    }
  )

  message("Data Analysis 2...")
  # Copy Data Analysis 2
  tryCatch(
    file_copy(
      path = create_path(base_path, da2_file),
      new_path = create_path(
        archive_path,
        paste0("data_analysis_2_backup", Sys.Date(), ".xlsx")
      ),
      overwrite = overwrite
    ),
    error = function(error) {
      error_notify(error = error, .file = "")

      try(rlang::cnd_signal(error))
    }
  )

  message("14-Day Trends...")
  # Copy 14-Day Trends
  tryCatch(
    file_copy(
      path = create_path(base_path, trends14_file),
      new_path = create_path(
        archive_path,
        paste0("14_day_trend_backup", Sys.Date(), ".xlsx")
      ),
      overwrite = overwrite
    ),
    error = function(error) {
      error_notify(error = error, .file = "")

      try(rlang::cnd_signal(error))
    }
  )

  message("Zipcode...")
  # Copy Zipcode
  tryCatch(
    file_copy(
      path = create_path(base_path, zipcode_file),
      new_path = create_path(
        archive_path,
        paste0("zipcode_backup", Sys.Date(), ".xlsx")
      ),
      overwrite = overwrite
    ),
    error = function(error) {
      error_notify(error = error, .file = "")

      try(rlang::cnd_signal(error))
    }
  )

  message("Finished copying files.")

  message("Deleting old backups...")
  # Delete old backups
  tryCatch(
    coviData::trim_backups(
      archive_path,
      pattern = ".*([.]xlsx|[.]sas)",
      min_backups = 7
    ),
    error = function(error){
      error_notify(error = error, .action = "deleting", .file = "old files")

      try(rlang::cnd_signal(error))
    }
  )

  message("Done.")
}

capture.output(
  run_script(),
  file = "C:/Users/Jesse.Smith/Documents/jobs/log/backup_analysis_files.log",
  append = TRUE,
  type = "message"
)
