### create_test_data.r ---
##
## Filename: create_test_data.r
## Description:
## Author: Christian A. Parker
## Maintainer:
## Created: Tue Oct 26 10:08:09 2021 (-0700)
## Version:
## Last-Updated:
##           By:
##     Update #: 9
## URL:
## Keywords:
## Compatibility:
##
######################################################################
##
### Commentary:
##
##
##
######################################################################
##
### Change Log:
##
##
######################################################################
##
### Code:
source("~/git/talloaks/talloaks-admin/scripts/google.r");

sheetId <- '1RSRrrR_LeB-t0tIjL4fhinsKP23fuRnpu__PIO9psYA'
rootDir <- 'staging/pending';

sheets <- c(
    "measurements_initial"
  , "measurements_v1"
  , "measurements_v1b"
  , "measurements_v2"
  , "measurements_v3"
  , "versions_v1"
  , "sensors_initial"
  , "sensors_update"
  , "locations"
)

for(sheet in sheets) {
    m <-readGoogleSheet(sheetId, sheet=sheet)
    write.csv(m, sprintf('%s/%s.csv', rootDir, sheet, na=""))
}



######################################################################
### create_test_data.r ends here
