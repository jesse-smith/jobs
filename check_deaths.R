run_script <- function() {

  coviData::ennotify_context("setting global options", inform = TRUE)
  options(rlang_backtrace_on_error= "full", error= quote(coviData::ennotify()))

  coviData::ennotify_context("setting ennotify options", inform = TRUE)
  coviData::ennotify_inform(TRUE)
  coviData::ennotify_to(
    "Jesse.Smith@shelbycountytn.gov",
    "Faisal.Mohamed@shelbycountytn.gov"
  )

  message("\n", Sys.time())

  coviData::ennotify_context("checking death linelists")
  # Cross-reference NBS linelist with Surveillance linelist and output differences
  coviData::check_deaths(save = TRUE)

  coviData::ennotify_context("deleting death linelist backups")
  # Delete old versions
  coviData::trim_backups(
    "V:/EPI DATA ANALYTICS TEAM/MORTALITY DATA/Missing IDs/",
    pattern = "missing_ids_.*[.]xlsx",
    min_backups = 7
  )
  message("Done.")
}

capture.output(
  run_script(),
  file = "C:/Users/Jesse.Smith/Documents/jobs/log/check_deaths.log",
  append = TRUE,
  type = "message"
)
