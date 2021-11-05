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
##     Update #: 5
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
rootDir <- '~/git/caparker/openaq-lcs-fetch/fetcher/data/cmu/staging/pending';

sheets <- c(
    "measurements_initial"
  , "measurements_v1"
  , "measurements_v1b"
  , "versions_v1"
  , "locations"
)

for(sheet in sheets) {
    m <-readGoogleSheet(sheetId, sheet=sheet)
    write.csv(m, sprintf('%s/%s.csv', rootDir, sheet))
}



######################################################################
### create_test_data.r ends here
