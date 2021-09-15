library(fs)
library(dplyr)
library(covidCluster)
options(rlang_backtrace_on_error = "branch", error = rlang::entrace)

run_script <- function() {

  error_notify <- function(error, .fn) {
    coviData::notify(
      to = "Allison.Plaxco@shelbycountytn.gov",
      subject = paste0(.fn, " Failed on ", Sys.Date()),
      body = paste0(
        "The 'download_integrated_data.R' script failed at ",
        "`", .fn, "` with the following error:\n\n",
        error$message, "\n\n",
        "See log for details."
      )
    )
  }

  message("\n", Sys.time())

  # Use `create_path()` from coviData
  create_path <- coviData::create_path

  message("Downloading data...")
  # Download new data
  tryCatch(
    download_integrated_data(),
    error = function(error) {
      error_notify(error = error, .fn = "download_integrated_data()")

      try(rlang::cnd_signal(error))
    }
  )
  message("Deleting old backups...")
  # Delete files older than 7 days
  tryCatch(
    coviData::trim_backups(
      paste0(
        "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
        "Integrated data tool Case Interviews/data/"
      ),
      pattern = "integrated_data_.*([.]csv|[.]xlsx)",
      min_backups = 7
    ),
    error = function(error) {
      error_notify(error = error, .fn = "trim_backups()")

      try(rlang::cnd_signal(error))
    }
  )

  message("Done!")
}

capture.output(
  run_script(),
  file = "C:/Users/Allison.Plaxco/Documents/jobs/log/download_integrated_data.log",
  append = TRUE,
  type =  "message"
)
