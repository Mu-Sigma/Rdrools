## ----echo = FALSE, message = FALSE, results = 'hide', warning = FALSE, error=FALSE, screenshot.force=FALSE----
#Package installation if required for handbook

if (!requireNamespace("DT", quietly = TRUE)) {
     install.packages("DT", repos = "http://cloud.r-project.org/")
     library("DT")
}

if (!requireNamespace("lubridate", quietly = TRUE)) {
     install.packages("lubridate", repos = "http://cloud.r-project.org/")
     library("lubridate")
}
if (!requireNamespace("plotly", quietly = TRUE)) {
     install.packages("plotly", repos = "http://cloud.r-project.org/")
     library("plotly")
}
library("Rdrools")

## ----setup, include=FALSE------------------------------------------------
options(stringsAsFactors = F)

## ------------------------------------------------------------------------
data("iris")
data("irisRules")
sampleRules <- irisRules
rownames(sampleRules) <- seq(1:nrow(sampleRules))
sampleRules[is.na(sampleRules)]    <-""
sampleRules

