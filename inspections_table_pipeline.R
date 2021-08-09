coviData::log_start(file = "insp_table_pipeline.log")

message("\n", Sys.time())

coviData::ennotify_set_options("Jesse.Smith@shelbycountytn.gov")

coviData::ennotify_context("inspections table pipeline")
covidReport::insp_table_pipeline()

coviData::ennotify_context("conditionally sending update")
if (weekdays(Sys.Date()) == "Thursday") {
  try(coviData::notify(
    to = c(coviData::ennotify_to(), "Austen.Onek@shelbycountytn.gov"),
    subject = paste0("COVID-19 Business Inspections Table (", Sys.Date(), ")"),
    body = paste0(
      'The COVID-19 Business Inspections Table is updated for today. ',
      'To access this table, please visit ',
      '<a href="file:///V:/Compliance/Inspection Data for Publishing/Table">',
      'V:/Compliance/Inspection Data for Publishing/Table',
      '</a> ',
      'on a Shelby County computer connected to the "V:" drive.',
      '<br><br>',
      'Thanks!',
      '<br><br>',
      "Jesse Smith",
      '<br><br>',
      '<i>Note: This email was generated automatically.</i>'
    ),
    html = TRUE
  ))
}

coviData::log_end()
