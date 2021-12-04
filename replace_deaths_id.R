run_script <- function() {
  coviData::ennotify_context("setting global options", inform = TRUE)
  options(rlang_backtrace_on_error= "full", error= quote(coviData::ennotify()))

  coviData::ennotify_context("setting ennotify options", inform = TRUE)
  coviData::ennotify_inform(TRUE)
  if (weekdays(Sys.Date()) == "Tuesday") {
    coviData::ennotify_to(
      "Allison.Plaxco@shelbycountytn.gov",
      "Liang.Li@shelbycountytn.gov"
    )
  } else {
    coviData::ennotify_to("Allison.Plaxco@shelbycountytn.gov")
  }

  message("\n", Sys.time())

  coviData::ennotify_context("replacing deaths IDs")
  coviData:::replace_deaths_id()
}

capture.output(
  run_script(),
  file = "C:/Users/Allison.Plaxco/Documents/jobs/log/replace_deaths_id.log",
  append = TRUE,
  type = "message"
)
