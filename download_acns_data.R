run_script <- function() {

  try(message("\n", Sys.time()))

  try(rlang::inform("Setting global options..."))
  try(
    options(rlang_backtrace_on_error="full", error=quote(coviData::ennotify()))
  )

  coviData::ennotify_context("setting ennotify options", inform = TRUE)
  coviData::ennotify_inform(TRUE)
  coviData::ennotify_to(
    "allison.plaxco@shelbycountytn.gov",
    "karim.gilani@shelbycountytn.gov",
    "Liang.Li@shelbycountytn.gov",
    "ITECDbAdmins@shelbycountytn.gov",
    "Rachel.Rice@shelbycountytn.gov",
    "Logan.Sell@shelbycountytn.gov"
  )

  # Load pipe
  coviData::ennotify_context("importing magrittr pipe")
  import::from("magrittr", "%>%")

  # Create notification functions
  coviData::ennotify_context("creating notification functions")
  notify_start <- function() {
    coviData::notify(
      to = coviData::ennotify_to(),
      subject = glue::glue("ACNS Data Download Starting ({Sys.Date()})"),
      body = paste0(
        "A new ACNS file has been found on the SFTP server; ",
        "this file is being prepared for import into the ACNS system.",
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

  notify_missing <- function(time, notified) {
    it_email <- c(
      "anthony.parker@shelbycountytn.gov",
      "troy.white@shelbycountytn.gov",
      "Jerrel.Moore@shelbycountytn.gov",
      "itecdbadmins@shelbycountytn.gov"
    )

    time <- lubridate::as_datetime(paste(lubridate::today(), time), tz = "")

    if (!notified && lubridate::now() >= time) {
      coviData::notify(
        to = c(coviData::ennotify_to(), it_email),
        subject = glue::glue(
          "Updated ACNS Data Not on SFTP Server ",
          "({format(Sys.Date(), '%a %m/%d/%Y')})"
        ),
        body = glue::glue(
          "This is an automated notification. ",
          "The ACNS data file does not seem to be updated; ",
          "please take a look at potential failures and adjust accordingly.",
          "\n\n",
          "This job will continue to look for a new data file every 5 minutes ",
          "until 9 PM (or until a new file is found)."
        )
      )
      TRUE
    } else if (notified) {
      TRUE
    } else {
      FALSE
    }
  }

  notify_end <- function() {
    coviData::notify(
      to = coviData::ennotify_to(),
      subject = paste0("ACNS Data Download Complete (", Sys.Date(), ")"),
      body = stringr::str_glue(
        "The ACNS file preparation is complete. You can find the latest file ",
        "to import at ",
        "<a href='file:///{covidsms::path_sms(combined = TRUE)}'>",
        "{covidsms::path_sms(combined = TRUE)}",
        "</a> ",
        "Please check your previous emails for any non-fatal errors.",
        "<br><br>",
        "<i>Note: This email was generated automatically.</i>"
      ),
      html = TRUE
    )
  }

  # Create insistent checking function
  coviData::ennotify_context("creating insistent ACNS data checking function")
  insistently_check_date_updated <- purrr::insistently(
    covidsms::acns_date_updated,
    rate = purrr::rate_backoff(max_times = 10L),
    quiet = FALSE
  )

  # Create upload function with notification
  coviData::ennotify_context("creating safe ACNS upload function")
  possibly_upload_acns <- purrr::possibly(
    covidsms::upload_acns,
    quiet = FALSE,
    otherwise = rlang::expr(coviData::notify(
      to = coviData::ennotify_to(),
      subject = paste0("ACNS Upload Failed (", Sys.Date(), ")"),
      body = paste0(
        "Uploading of ACNS data has failed. Data is ready for upload; ",
        "please do so manually before 6:30 PM to ensure notifications are sent."
      )
    ))
  )

  # Create repeating check/download function
  coviData::ennotify_context("creating iteration function")
  try_download <- function() {
    # Check for new file; start download if `TRUE`
    date_updated <- insistently_check_date_updated()
    is_updated <- date_updated == Sys.Date()

    if (is_updated) {

      coviData::ennotify_context("sending ACNS start notification")
      try(notify_start())

      coviData::ennotify_context("downloading data")
      acns_dwnld <- eval(covidsms::download_acns())

      coviData::ennotify_context("preparing data and adding NBS")
      acns_prep  <- eval(covidsms::prep_acns(acns_dwnld))

      coviData::ennotify_context("translating to ACNS format")
      acns_trans <- eval(covidsms::translate_acns_upload(acns_prep))

      coviData::ennotify_context("archiving ACNS data")
      eval(covidsms::archive_acns_upload(acns_trans))

      # coviData::ennotify_context("uploading ACNS notification data")
      # upload_rtn <- eval(possibly_upload_acns(acns_trans))
      # eval(upload_rtn)

      coviData::ennotify_context("sending ACNS end notification")
      try(notify_end())
    }
    is_updated
  }

  # Stop trying at 9 pm
  end_time <- lubridate::today() %>%
    paste("21:00:00") %>%
    lubridate::as_datetime(tz = "")

  # Wait 5 minutes between tries
  wait <- 300L

  # Number of tries
  times <- lubridate::interval(lubridate::now(), end_time) %>%
    lubridate::as.period(unit = "minutes") %>%
    .@minute %>%
    max(0L)

  # Slow down the download checking function
  try_download_slowly <- purrr::slowly(
    try_download,
    rate = purrr::rate_delay(pause = wait, max_times = times)
  )

  # Checking for new SMS data
  coviData::ennotify_context("checking for new data")
  is_updated <- FALSE
  notified <- FALSE
  while(!is_updated) {
    is_updated <- try_download_slowly()
    if (is_updated) notified <- TRUE
    notified   <- notify_missing("14:15:00", notified = notified)
  }
  rlang::inform("Done.")
}

capture.output(
  run_script(),
  file = "C:/Users/Allison.Plaxco/Documents/jobs/log/download_acns_data.log",
  append = TRUE,
  type = "message"
)
