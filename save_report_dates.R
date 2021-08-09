run_script <- function() {

  coviData::ennotify_inform(TRUE)

  coviData::ennotify_context("setting global options", inform = TRUE)
  options(rlang_backtrace_on_error= "full", error= quote(coviData::ennotify()))

  coviData::ennotify_context("setting ennotify options", inform = TRUE)
  coviData::ennotify_inform(TRUE)
  coviData::ennotify_to("jesse.smith@shelbycountytn.gov")

  message("\n", Sys.time())

  coviData::ennotify_context("importing magrittr pipe")
  import::from("magrittr", "%>%")

  # ----------------------------------------------------------------------------

  # Pull new report date data from NBS files
  coviData::ennotify_context("archiving report date")
  coviData::archive_report_date()

  # Combine data
  coviData::ennotify_context("combining report date data")
  coalesced_data <- coviData::coalesce_report_date()

  # Add specimen collection dates for convenience
  coviData::ennotify_context("adding collection date to report date")
  date <- coviData::path_inv() %>%
    fs::path_file() %>%
    fs::path_ext_remove() %>%
    stringr::str_extract("[0-9]{1,4}.?[0-9]{1,2}.?[0-9]{1,4}") %>%
    lubridate::as_date()
  report_and_collection_data <- coviData::add_collection_date(
    coalesced_data,
    date = date
  )

  # Save combined data
  coviData::ennotify_context("saving combined data")
  coviData::save_report_date(report_and_collection_data)

  # Check that collection date is not missing
  coviData::ennotify_context("checking collection date")
  check_collection_date <- all(
    is.na(coviData::load_report_date()[["collection_date"]])
  )
  if (check_collection_date) {
    coviData::notify(
      to = coviData::ennotify_to(),
      subject = "`collection_date` parsed unsuccessfully",
      body = "See log for any details that were captured."
    )
  }
}

capture.output(
  run_script(),
  file = "C:/Users/Jesse.Smith/Documents/jobs/log/save_report_dates.log",
  append = TRUE,
  type =  "message"
)
