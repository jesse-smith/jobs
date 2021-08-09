
download_and_replace_ael <- rlang::expr({

  message("\n", Sys.time())

  in_file <- "in 'download_ael_data.R'"

  message("Importing functions from coviData...")
  tryCatch(
    import::from(coviData, download_ael, replace_ael, error_notify),
    error = coviData::error_notify(
      operation = paste("`import::from(...)`", in_file)
    )
  )

  message("Downloading AEL data...")
  tryCatch(
    download_ael(),
    error = error_notify(operation = paste("`download_ael()`", in_file))
  )

  message("Replacing AEL data...")
  tryCatch(
    replace_ael(),
    error = error_notify(operation = paste("`replace_ael()`", in_file))
  )

  message("Done.")
})

capture.output(
  eval(download_and_replace_ael),
  file = "C:/Users/Jesse.Smith/Documents/jobs/log/download_ael_data.log",
  append = TRUE,
  type = "message"
)
