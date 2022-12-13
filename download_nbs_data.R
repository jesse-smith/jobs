# Setup ------------------------------------------------------------------------
# Log messages/warnings/errors
coviData::log_start("download_nbs_data.log", dir = "C:/Users/allison.plaxco/Documents/jobs/log")
on.exit(coviData::log_end(), add = TRUE)

# Log start time
message("\n", Sys.time())

# Set `ennotify()` options for error tracing/notification
coviData::ennotify_set_options(
  "Allison.Plaxco@shelbycountytn.gov",
  "Liang.Li@shelbycountytn.gov",
  "Rachel.Rice@shelbycountytn.gov",
  "Jennifer.Kmet@shelbycountytn.gov",
  "Faisal.Mohamed@shelbycountytn.gov",
  "Richard.Ewool@shelbycountytn.gov"
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

#Add this when you want to download that additional file
insist_download_extra_nbs_snapshot <- purrr::insistently(
  coviData:::download_extra_case_file
)


# insist_convert_nbs_snapshot <- purrr::insistently(
#   coviData::convert_nbs_snapshot
# )

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
  # insist_convert_nbs_snapshot(force = TRUE)
  # Sys.sleep(3)
}

#add this when ready to split the file
download_extra_nbs <- function() {
  insist_download_extra_nbs_snapshot(force = TRUE)
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
    coviData::ennotify_context("downloading extra investigation data")
    download_extra_nbs()

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
gc(verbose = FALSE)



# Combine the two parts of the case file and size ---------------------------------


#read both of the files
cases1 <- data.table::fread(file = paste0(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "Sandbox data pull Final/", Sys.Date(), " Final Data Pull_part1.csv"
), colClasses = "character", data.table= TRUE)


cases2 <- data.table::fread(file = paste0(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "Additional case file/", Sys.Date(), " Additional Data Pull.csv"
), colClasses = "character", data.table= TRUE)

merged <- rbind(cases1, cases2)


data.table::fwrite(merged, paste0(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "Sandbox data pull Final/", Sys.Date(), " Final Data Pull.csv"),
       sep=",", na="", compress = "none",scipen=999, eol="\n")

#remove the extra file
file.remove(paste0(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "Additional case file/", Sys.Date(), " Additional Data Pull.csv"
))
file.remove(paste0(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "Sandbox data pull Final/", Sys.Date(), " Final Data Pull_part1.csv"
))

# Outputs ----------------------------------------------------------------------
# Load data
inv <- coviData::process_inv()
pcr <- coviData::process_pcr(inv = inv)
gc(verbose = FALSE)

# Save SAS data
#coviData::write_sas_nbs(inv = inv, pcr = pcr)

# Create daily report
coviData::ennotify_context("creating daily report")
covidReport::rpt_daily_pptx(inv = inv, pcr = pcr)
covidReport:::rpt_weekly_pptx(inv = inv, pcr = pcr)
gc(verbose = FALSE)


# Create Google Sheets output
coviData::ennotify_context("creating google sheets timeseries")
coviData::write_file_delim(
  covidReport::gs_timeseries(inv = inv, pcr = pcr),
  path = paste0(
    "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/gs_timeseries_", coviData::date_inv(), ".csv"
  ),
  force = TRUE
)

# Create demographic report
coviData::ennotify_context("creating demographic report")
covidReport::rpt_demog_pptx(inv = coviData::pos(inv))
gc(verbose = FALSE)



# Remove unneeded data
pos_inv <- coviData::pos(inv)
#remove(inv)
gc(verbose = FALSE)

# # Switch to report errors to Allison only (except Thursdays)
# if (weekdays(lubridate::today()) != "Thursday") {
#   coviData::ennotify_to(
#     "Allison.Plaxco@shelbycountytn.gov"
#   )
# }

# Maps first so I can remove `pcr`

# Testing rate map
test_map <- covidReport::test_map_rate(pcr)
gc(verbose = FALSE)
path_test_map <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "jtf_figs/test_map/", paste0("test_map_", coviData::date_inv()),
  ext = "png"
)
coviData::save_plot(test_map, path = path_test_map, ratio = c(12,9), size = 1.125)


# Testing rate map, grant zip highlighted
grant_test_map <- covidReport:::grant_zip_test_map_rate(pcr)
gc(verbose = FALSE)
path_test_map <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "jtf_figs/test_map/", paste0("grant_test_map_", coviData::date_inv()),
  ext = "png"
)
coviData::save_plot(grant_test_map, path = path_test_map, ratio = c(12,9), size = 1.125)
#remove(pcr)
gc(verbose = FALSE)



# Active case rate map
active_map <- covidReport::active_map_rate(pos_inv)
path_active_map <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "jtf_figs/active_case_map/", paste0("active_case_map_", coviData::date_inv()),
  ext = "png"
)
coviData::save_plot(active_map, path = path_active_map, ratio = c(12,9), size = 1.125)


# Active case rate map
grant_active_map <- covidReport:::grant_zip_active_map_rate(pos_inv)
path_active_map <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "jtf_figs/active_case_map/", paste0("grant_active_case_map_", coviData::date_inv()),
  ext = "png"
)
coviData::save_plot(grant_active_map, path = path_active_map, ratio = c(12,9), size = 1.125)

# Save map output for Google Sheets

active_map[["data"]] %>%
  tidyr::drop_na(.data[["zip"]]) %>%
  tidyr::replace_na(list(n = 0L)) %>%
  dplyr::select("zip", "n") %>%
  dplyr::left_join(
    test_map[["data"]],
    by = "zip",
    suffix = c("_active", "_test")
  ) %>%
  dplyr::select("zip", "n_active", "n_test") %>%
  coviData::write_file_delim(paste0(
    "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/gs_zips",coviData::date_inv(),".csv")
  )

# Update deaths linelist
pos_inv %>%
  dplyr::select(
    "jurisdiction_nm",
    "die_from_illness_ind",
    "inv_case_status",
    "patient_local_id",
    "patient_first_name",
    "patient_last_name",
    "patient_current_sex",
    "patient_deceased_ind",
    "patient_deceased_dt",
    "patient_ethnicity",
    "patient_race_calc",
    "age_in_years",
    "alt_county",
    "patient_dob",
    "specimen_coll_dt"
  ) %>%
  dplyr::mutate(
    patient_deceased_dt = .data[["patient_deceased_dt"]] %>%
      lubridate::ymd_hms() %>%
      lubridate::as_date()
  ) %>%
  dplyr::filter(.data[["patient_deceased_ind"]] %in% c("Y", "Yes")) %>%
  dplyr::rename_with(stringr::str_to_upper) %>%
  openxlsx::write.xlsx(coviData:::path_deaths("nbs"), overwrite = TRUE)

# Save report dates
coviData::ennotify_context("archiving report date")
coviData::archive_report_date()
coviData::ennotify_context("coalescing report date")
coalesced <- coviData::coalesce_report_date()
coviData::ennotify_context("adding collection date to report date")
collection_date <- coviData::add_collection_date(coalesced)
remove(coalesced)
gc(verbose = FALSE)
coviData::ennotify_context("saving report date linelist")
coviData::save_report_date(collection_date)
remove(collection_date)
gc(verbose = FALSE)

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
gc(verbose = FALSE)
rt_plot <- covidModel::plot_rt(rt, .rough_rt = rough_rt)
rt_path <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/jtf_figs/rt_fig",
  paste0("rt_plot", coviData::date_inv()),
  ext = "svg"
)
if (rlang::is_interactive()) show(rt_plot)
#covidReport::save_plot(rt_plot, path = rt_path, force = TRUE)

ggplot2::ggsave(
  rt_path,
  plot = rt_plot,
  device = "svg",
  width = 16,
  height = 9,
  dpi = 300
)

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
    gt::fmt_markdown(columns = dplyr::everything(), rows = c(F,F,T,T,T)) %>%
    {show(.); .} %>%
    gt::gtsave(
      coviData::path_create(
        "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/jtf_figs/rt_table",
        paste0("rt_table_", coviData::date_inv()),
        ext = "png"
      )
    )
}


# Active ped case rate map
active_ped_map <- covidReport:::active_ped_map_rate()
path_active_ped_map <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "jtf_figs/active_ped_map/", paste0("active_ped_map_", coviData::date_inv()),
  ext = "png"
)
coviData::save_plot(active_ped_map, path = path_active_ped_map, ratio = c(12,9), size = 1.125)

active_ped_map[["data"]] %>%
  tidyr::drop_na(.data[["zip"]]) %>%
  tidyr::replace_na(list(n = 0L)) %>%
  dplyr::select("zip", "n") %>%
  dplyr::rename(
    n_active = n
  ) %>%
  dplyr::select("zip", "n_active") %>%
  coviData::write_file_delim(paste0(
    "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/PED_only_gs_zips",coviData::date_inv(),".csv")
  )

#add testing sites to the maps
covidReport:::add_ts_grant_active_map()
covidReport:::add_ts_grant_test_map()
covidReport:::add_ts_active_map()
covidReport:::add_ts_ped_map()
covidReport:::add_ts_test_map()


# Send status update email
coviData::ennotify_context("sending daily email")
to <- coviData::ennotify_to()
# if (weekdays(lubridate::today()) == "Thursday") {
#   to <- c(to, "Jennifer.Kmet@shelbycountytn.gov")
# }
covidReport::rpt_daily_mail(to = to, inv = inv, pcr = pcr)
covidReport:::rpt_weekly_mail(to = to, inv = inv, pcr = pcr)
