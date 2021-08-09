# Setup ------------------------------------------------------------------------
# Log messages/warnings/errors
coviData::log_start("download_nbs_data.log")
on.exit(coviData::log_end(), add = TRUE)

# Log start time
message("\n", Sys.time())

# Set `ennotify()` options for error tracing/notification
coviData::ennotify_set_options(
  "Chaitra.Subramanya@shelbycountytn.gov",
  "Allison.Plaxco@shelbycountytn.gov",
  "Jesse.Smith@shelbycountytn.gov",
  "Liang.Li@shelbycountytn.gov"
)

# Import -----------------------------------------------------------------------
coviData::ennotify_context("importing magrittr pipe")
import::from("magrittr", "%>%")

# Notification functions -------------------------------------------------------
coviData::ennotify_context("creating NBS download notification functions")

notify_start <- function() {

  subject <- paste0("Starting Data for Regions Download - ", Sys.Date())

  body <- paste0(
    "Data for the MSR region is being downloaded; this includes ",
    "investigation data (NBS snapshot), PCR test data (PCR snapshot), ",
    "antigen test data (antigen snapshot), and antibody test data ",
    "(serology snapshot) for ", Sys.Date(), ".",
    "<br><br>",
    "You will be notified when the download is complete or there is an error ",
    "in the download process. The full download can take up to 1 hour due ",
    "to slow transfer speeds to and from the 'V:' drive.",
    "<br><br>",
    "Stay tuned!",
    "<br><br>",
    "<i>Note: This is an automated notification</i>"
  )

  coviData::notify(
    to = coviData::ennotify_to(),
    subject = subject,
    body = body,
    html = TRUE
  )
}

notify_finish <- function() {

  subject <- paste0("Finished Data for Regions Download - ", Sys.Date())

  body <- paste0(
    "Data for the MSR region has finished downloading; files for ", Sys.Date(),
    " should now be available. Please check your previous emails ",
    "for notifications regarding files that did not download correctly.",
    "<br><br>",
    "<i>Note: This is an automated notification</i>"
  )

  coviData::notify(
    to = coviData::ennotify_to(),
    subject = subject,
    body = body,
    html = TRUE
  )
}

# Download Functions -----------------------------------------------------------
coviData::ennotify_context("creating NBS download wrapper functions")

# Insistent versions of all functions (in case of V drive connection failure)
insist_download_nbs_snapshot <- purrr::insistently(
  coviData::download_nbs_snapshot
)

insist_convert_nbs_snapshot <- purrr::insistently(
  coviData::convert_nbs_snapshot
)

insist_download_pcr_snapshot <- purrr::insistently(
  coviData::download_pcr_snapshot
)

insist_download_antigen <- purrr::insistently(
  coviData::download_antigen_snapshot
)

insist_download_serology <- purrr::insistently(
  coviData::download_serology_snapshot
)

download_nbs <- function() {
  insist_download_nbs_snapshot(force = TRUE)
  Sys.sleep(3)
  insist_convert_nbs_snapshot(force = TRUE)
  Sys.sleep(3)
}

download_pcr <- function() {
  insist_download_pcr_snapshot(force = TRUE)
  Sys.sleep(3)
}

download_antigen <- function() {
  try(insist_download_antigen(force = TRUE))
  Sys.sleep(3)
}

download_serology <- function() {
  try(insist_download_serology(force = TRUE))
  Sys.sleep(3)
}

# Iteration Function ---------------------------------------------------------
coviData::ennotify_context("creating NBS download iteration function")

try_download <- function() {

  # Check for new file
  is_updated <- coviData::check_date_updated(quiet = TRUE)

  if (is_updated) {

    notify_start()

    coviData::ennotify_context("downloading antigen data")
    download_antigen()
    coviData::ennotify_context("downloading serology data")
    download_serology()
    coviData::ennotify_context("downloading investigation data")
    download_nbs()
    coviData::ennotify_context("downloading PCR data")
    download_pcr()

    notify_finish()
  }

  rlang::is_true(is_updated)
}

# Download ---------------------------------------------------------------------
coviData::ennotify_context("determining when to stop looking for files")
end_time <- lubridate::as_datetime(
  paste(Sys.Date()+1L, "06:00:00"),
  tz = Sys.timezone()
)
minutes <- lubridate::interval(Sys.time(), end_time) %>%
  lubridate::as.period(unit = "minutes") %>%
  .@minute %>%
  max(0L)

# Slow down download function to once per minute
coviData::ennotify_context("create slow download function")
try_download_slowly <- purrr::slowly(
  try_download,
  rate = purrr::rate_delay(pause = 60, max_times = minutes)
)

coviData::ennotify_context("downloading NBS data")
is_updated <- FALSE
while (!is_updated) {
  is_updated <- try_download_slowly()
}
gc()

# Outputs ----------------------------------------------------------------------
# Load data
inv <- coviData::process_inv(replace = TRUE)
pcr <- coviData::process_pcr(inv = inv)
gc()

# Create daily report
coviData::ennotify_context("creating daily report")
covidReport::rpt_daily_pptx(inv = inv, pcr = pcr)
gc()

# Create Google Sheets output
coviData::ennotify_context("creating google sheets timeseries")
coviData::write_file_delim(
  covidReport::gs_timeseries(inv = inv, pcr = pcr),
  path = paste0(
    "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/gs_timeseries.csv"
  ),
  force = TRUE
)

# Create demographic report
if (weekdays(lubridate::today()) == "Tuesday") {
  coviData::ennotify_context("creating demographic report")
  covidReport::rpt_demog_pptx(inv = coviData::pos(inv))
  gc()
}

# Send status update email
coviData::ennotify_context("sending daily email")
to <- coviData::ennotify_to()
if (weekdays(lubridate::today()) == "Thursday") {
  to <- c(to, "Jennifer.Kmet@shelbycountytn.gov")
}
covidReport::rpt_daily_mail(to = to, inv = inv, pcr = pcr)

# Remove unneeded data
pos_inv <- coviData::pos(inv)
remove(inv, pcr)
gc()

# Switch to report errors to Jesse only (except Thursdays)
if (weekdays(lubridate::today()) != "Thursday") {
  coviData::ennotify_to(
    "Jesse.Smith@shelbycountytn.gov",
    "Allison.Plaxco@shelbycountytn.gov"
  )
}

# Save report dates
coviData::ennotify_context("archiving report date")
coviData::archive_report_date()
coviData::ennotify_context("coalescing report date")
coalesced <- coviData::coalesce_report_date()
coviData::ennotify_context("adding collection date to report date")
collection_date <- coviData::add_collection_date(coalesced)
remove(coalesced)
gc()
coviData::ennotify_context("saving report date linelist")
coviData::save_report_date(collection_date)
remove(collection_date)
gc()

# Create Rt figure
coviData::ennotify_context("modeling Rt")
rt_data <- dplyr::semi_join(
  dplyr::as_tibble(coviData::load_report_date()),
  pos_inv,
  by = "inv_local_id"
) %>% dplyr::mutate(collection_date = lubridate::as_date(collection_date))

rt <- covidModel::estimate_rt(rt_data)
rough_rt <- covidModel::estimate_rt(rt_data, trend = 1L, boost = FALSE)
remove(rt_data)
gc()
rt_plot <- covidModel::plot_rt(rt, .rough_rt = rough_rt)
rt_path <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/jtf_figs/rt_fig",
  paste0("rt_plot", Sys.Date()),
  ext = "png"
)
if (rlang::is_interactive()) show(rt_plot)
covidReport::save_plot(rt_plot, path = rt_path, force = TRUE)

if (weekdays(lubridate::today()) == "Thursday") {
  coviData::ennotify_context("creating Rt table")
  current_rt <- rt %>%
    dplyr::slice_tail(n = 1) %>%
    dplyr::pull(".pred") %>%
    round(digits = 2) %>%
    as.character()

  active <- covidReport:::case_calc_active(pos_inv) %>%
    dplyr::filter(status == "Active") %>%
    dplyr::pull("n") %>%
    format(big.mark = ",")

  rt_tbl_val <- covidModel:::simulate_infections(rt, h = 30) %>%
    vctrs::vec_slice(i = seq(vctrs::vec_size(.)-29, vctrs::vec_size(.), 1)) %>%
    cumsum() %>%
    vctrs::vec_slice(i = vctrs::vec_size(.) - c(19, 9, 0)) %>%
    round() %>%
    format(big.mark = ",") %>%
    stringr::str_squish() %>%
    {paste0("**", ., "**")} %>%
    purrr::prepend(c(current_rt, active))

  rt_tbl_nm <- c(
    "Rt",
    "Active Cases",
    "**Cases over next 10 Days**",
    "**Cases over next 20 Days**",
    "**Cases over next 30 Days**"
  )
  rt_tbl <- tibble::tibble(`Cases` = rt_tbl_nm, Count = rt_tbl_val)

  title <- paste0("Implications of Current Rt and Active Cases")
  gt::gt(rt_tbl) %>%
    gt::tab_header(title = title) %>%
    gt::opt_row_striping() %>%
    gt::cols_label(Cases = "", Count = "") %>%
    gt::fmt_markdown(columns = dplyr::everything(), rows = c(F,F,T,T,T)) %T>%
    {show(.)} %>%
    gt::gtsave(
      coviData::path_create(
        "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/jtf_figs/rt_table",
        paste0("rt_table_", Sys.Date()),
        ext = "png"
      )
    )
}
