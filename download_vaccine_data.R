# Setup ------------------------------------------------------------------------
# Log messages/warnings/errors
coviData::log_start("download_vaccine_data.log", dir = "C:/Users/allison.plaxco/Documents/jobs/log")
on.exit(coviData::log_end(), add = TRUE)

# Log start time
message("\n", Sys.time())

# Set up `ennotify()` options for error tracing/notification
coviData::ennotify_set_options(
  "Allison.Plaxco@shelbycountytn.gov",
  "Liang.Li@shelbycountytn.gov",
  "Rachel.Rice@shelbycountytn.gov",
  "Jennifer.Kmet@shelbycountytn.gov"
)

# Import -----------------------------------------------------------------------
# Load pipe
coviData::ennotify_context("importing magrittr pipe")
import::from("magrittr", "%>%")

# Notification functions -------------------------------------------------------
coviData::ennotify_context("creating notification functions")
notify_start <- function() {
  coviData::notify(
    to = coviData::ennotify_to(),
    subject = paste0("Vaccine Data Download Starting (", Sys.Date(), ")"),
    body = paste0(
      "A new vaccination file has been uploaded to the state REDcap; ",
      "this file is being downloaded.",
      "<br><br>",
      "You will receive a follow-up email upon ",
      "completion, error, or missing data.",
      "<br><br>",
      "Stay tuned!",
      "<br><br>",
      "<i>Note: This email was generated automatically.</i>"
    ),
    html = TRUE
  )
}

notify_end <- function() {
  coviData::notify(
    to = coviData::ennotify_to(),
    subject = paste0("Vaccine Data Download Complete (", Sys.Date(), ")"),
    body = stringr::str_glue(
      "The vaccine data download is complete.",
      "Please check your previous emails for any non-fatal errors.",
      "<br><br>",
      "<i>Note: This email was generated automatically.</i>"
    ),
    html = TRUE
  )
}

# Check/Download functions -----------------------------------------------------
# Create insistent checking function (repeats of connection fails)
coviData::ennotify_context("creating insistent vaccine checking function")
insist_check_date_updated <- purrr::insistently(
  coviData::check_date_updated
)

coviData::ennotify_context("creating insistent vaccine download function")
insist_download_vaccine_snapshot <- purrr::insistently(
  coviData::download_vaccine_snapshot
)

# Create repeating check/download function
coviData::ennotify_context("creating iteration function")
try_download <- function() {
  # Check for new file; start download if `TRUE`
  is_updated <- insist_check_date_updated(vac = TRUE)

  if (is_updated) {
    # Download ACNS Data
    coviData::ennotify_context("downloading vaccine data")
    try(notify_start())
    insist_download_vaccine_snapshot()
    try(notify_end())
  }

  rlang::is_true(is_updated)
}

# Download ---------------------------------------------------------------------

# Stop trying at 6 am next day
end_time <- lubridate::as_datetime(
  paste(lubridate::today()+1, "06:00:00"),
  tz = Sys.timezone()
)

# Wait 5 minutes between checks
wait_minutes <- 5L

# Number of tries
times <- lubridate::interval(lubridate::now(), end_time) %>%
  lubridate::as.period(unit = "minutes") %>%
  .@minute %>%
  magrittr::divide_by_int(wait_minutes) %>%
  max(0L)

# Slow down the download checking function by `wait_minutes`
try_download_slowly <- purrr::slowly(
  try_download,
  rate = purrr::rate_delay(pause = floor(wait_minutes * 60L), max_times = times)
)

# Check for data
coviData::ennotify_context("checking for new vaccine data")
is_updated <- FALSE
while(!is_updated) {
  is_updated <- try_download_slowly()
}

# Outputs ----------------------------------------------------------------------
# Load vaccine data
v_data <- coviData::vac_prep(coviData::read_vac())

# Create vaccination goal plot
coviData::ennotify_context("creating vaccination goal figure")
v_goal_file_stem <- paste0("vaccination_goal_", Sys.Date())
v_goal_path <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "COVID-19 Vaccine Reporting/figs/vaccination_goal/",
  v_goal_file_stem,
  ext = "png"
)
p_goal <- covidReport::vac_plot_goal(v_data)
if (rlang::is_interactive()) show(p_goal)
covidReport::save_plot(p_goal, path = v_goal_path, width = 12, height = 9)

# Create vaccination age group plot
coviData::ennotify_context("creating vaccination age group figure")
v_age_pop_file_stem <- paste0("vaccination_age_pop_", Sys.Date())
v_age_pop_path <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "COVID-19 Vaccine Reporting/figs/vaccination_age_pop/",
  v_age_pop_file_stem,
  ext = "png"
)
p_age_pop <- covidReport::vac_plot_age(v_data)
if (rlang::is_interactive()) show(p_age_pop)
covidReport::save_plot(p_age_pop, path = v_age_pop_path)

# Create ZIP Map
coviData::ennotify_context("creating vaccination percent zip map")
v_map_pct_file_stem <- paste0("vaccination_zip_pct_", Sys.Date())
v_map_pct_path <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "COVID-19 Vaccine Reporting/figs/vaccination_map_pct/",
  v_map_pct_file_stem,
  ext = "png"
)
p_map_pct <- covidReport::vac_map_pct(v_data)
if (rlang::is_interactive()) show(p_map_pct)
coviData::save_plot(p_map_pct, path = v_map_pct_path, ratio = c(12, 9), size = 1.125)


coviData::ennotify_context("creating full vaccination percent zip map")
v_map_pct_file_stem <- paste0("full_vaccination_zip_pct_", Sys.Date())
v_map_pct_path <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "COVID-19 Vaccine Reporting/figs/vaccination_map_pct/",
  v_map_pct_file_stem,
  ext = "png"
)
p_map_pct_f <- covidReport:::vac_fully_map_pct(v_data)
if (rlang::is_interactive()) show(p_map_pct_f)
coviData::save_plot(p_map_pct_f, path = v_map_pct_path, ratio = c(12, 9), size = 1.125)


coviData::ennotify_context("creating vaccination percent zip map with grant zips")
v_map_pct_file_stem <- paste0("grant_vaccination_zip_pct_", Sys.Date())
v_map_pct_path <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "COVID-19 Vaccine Reporting/figs/vaccination_map_pct/",
  v_map_pct_file_stem,
  ext = "png"
)
p_map_pct_grant <- covidReport:::grant_zip_vac_map_pct(v_data)
if (rlang::is_interactive()) show(p_map_pct_grant)
coviData::save_plot(p_map_pct_grant, path = v_map_pct_path, ratio = c(12, 9), size = 1.125)


coviData::ennotify_context("creating full vaccination percent zip map with grant zips")
v_map_pct_file_stem <- paste0("grant_full_vaccination_zip_pct_", Sys.Date())
v_map_pct_path <- coviData::path_create(
  "V:/EPI DATA ANALYTICS TEAM/COVID SANDBOX REDCAP DATA/",
  "COVID-19 Vaccine Reporting/figs/vaccination_map_pct/",
  v_map_pct_file_stem,
  ext = "png"
)
p_map_pct_grant_f <- covidReport:::grant_zip_vac_fully_map_pct(v_data)
if (rlang::is_interactive()) show(p_map_pct_grant_f)
coviData::save_plot(p_map_pct_grant_f, path = v_map_pct_path, ratio = c(12, 9), size = 1.125)


# Email Vaccination Numbers
date = NULL
coviData::ennotify_context("summarizing and sending vaccination numbers")
recent_table <- gt::as_raw_html(covidReport:::vac_table_recent_email(date = NULL))

dose_table <- gt::as_raw_html(covidReport:::vac_table_totals_email(date = NULL))

Sys.sleep(10L)

coviData::notify(
  to = coviData::ennotify_to(),
  subject = paste("Vaccination Numbers for ", Sys.Date()),
  body = paste0(
    "Below are vaccination numbers for today.<br><br>",
    recent_table,
    "<br>",
    dose_table
  ),
  html = TRUE
)

rlang::inform("Done.")



# Report ----------------------------------------------------------------------
covidReport:::rpt_vac_pptx()
