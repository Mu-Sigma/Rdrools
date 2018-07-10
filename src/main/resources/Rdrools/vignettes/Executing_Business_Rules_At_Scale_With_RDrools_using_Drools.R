## ----echo = FALSE, message = FALSE, results = 'hide', warning = FALSE, error=FALSE, screenshot.force=FALSE----
#Package installation if required for handbook
required.packages <- c('rJava','Rdrools', 'Rdroolsjars','DT','lubridate','tidyverse','plotly')

for(pkg in required.packages){
  if(!require(pkg,character.only = T)){
    install.packages(pkg, repos = "http://cloud.r-project.org/")
    require(pkg)
  }
}


## ----setup, include=FALSE------------------------------------------------
options(stringsAsFactors = F)

## ------------------------------------------------------------------------
data(iris)
data("irisRules")
sampleRules <- irisRules
rownames(sampleRules) <- seq(1:nrow(sampleRules))
sampleRules[is.na(sampleRules)]    <-""
sampleRules

