coviData:::log_start("download_state_contacts.log")

coviData::ennotify_context("set global options", inform = TRUE)
options(rlang_backtrace_on_error = "full", error = quote(coviData::ennotify()))

coviData::ennotify_context("set ennotify options", inform = TRUE)
coviData::ennotify_inform(TRUE)
coviData::ennotify_to("Jesse.Smith@shelbycountytn.gov")

coviData::ennotify_context("download state monitoring contacts")
coviData::download_state_contacts()

coviData:::log_end()
