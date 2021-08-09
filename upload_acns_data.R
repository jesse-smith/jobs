run_script <- function() {

  coviData::ennotify_context("setting global options", inform = TRUE)
  options(rlang_backtrace_on_error= "full", error= quote(coviData::ennotify()))

  coviData::ennotify_context("setting ennotify options", inform = TRUE)
  coviData::ennotify_inform(TRUE)
  coviData::ennotify_to("Jesse.Smith@shelbycountytn.gov")

  message("\n", Sys.time())

  positive_data <- covidsms::load_positive()
  prepped_data <- covidsms::prep_positive(positive_data)
  translated_data <- covidsms::translate_acns_upload(prepped_data)
  covidsms::upload_positive(translated_data)
}

capture.output(
  run_script(),
  file = "C:/Users/Jesse.Smith/Documents/jobs/log/upload_acns_data.log",
  append = TRUE,
  type =  "message"
)
