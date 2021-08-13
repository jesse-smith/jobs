coviData::log_start("assign_acns.log")
on.exit(coviData::log_end(), add = TRUE)

message("\n", Sys.time())

coviData::ennotify_set_options(
  "Jesse.Smith@shelbycountytn.gov",
  "Karim.Gilani@shelbycountytn.gov",
  "Faisal.Mohamed@shelbycountytn.gov",
  "Allison.Plaxco@shelbycountytn.gov",
  "Liang.Li@shelbycountytn.gov"
)

# Load pipe
coviData::ennotify_context("importing magrittr pipe")
import::from("magrittr", "%>%")

# Preparing data
coviData::ennotify_context("preparing NBS assignment data")
nbs_data <- covidsms::prep_positive(filter_lab = FALSE, filter_new = TRUE)
gc(verbose = FALSE)
prep_nbs_data <- covidsms::prep_acns(
  nbs_data,
  incl_positive = FALSE,
  filter_acns = FALSE,
  filter_lab = FALSE,
  assign = TRUE
)
gc(verbose = FALSE)

coviData::ennotify_context("translating assignment data to REDcap format")
trans_data <- covidassign::translate_acns(prep_nbs_data, days = 10L)
gc(verbose = FALSE)

coviData::ennotify_context("assigning cases")
assign_data <- covidassign::assign_acns(trans_data)
gc(verbose = FALSE)

try(covidassign::validate_assignments(
  assign_data,
  notify_to = coviData::ennotify_to()
))

coviData::ennotify_context("preparing assignments for upload")
prep_data <- covidassign::prep_acns_redcap(assign_data)
gc(verbose = FALSE)

coviData::ennotify_context("uploading assignments")
upload_data <- covidassign::upload_assignments(prep_data)
gc(verbose = FALSE)
try(covidassign::validate_upload(
  upload_data,
  notify_to = coviData::ennotify_to()
))
