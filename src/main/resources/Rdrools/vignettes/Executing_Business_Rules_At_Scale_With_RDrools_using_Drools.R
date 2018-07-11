## ----echo = FALSE, message = FALSE, results = 'hide', warning = FALSE, error=FALSE, screenshot.force=FALSE----
#Package installation if required for handbook

if (!requireNamespace("DT", quietly = TRUE)) {
     install.packages("DT", repos = "http://cloud.r-project.org/")
}

if (!requireNamespace("lubridate", quietly = TRUE)) {
     install.packages("lubridate", repos = "http://cloud.r-project.org/")
}
if (!requireNamespace("ggplot2", quietly = TRUE)) {
     install.packages("ggplot2", repos = "http://cloud.r-project.org/")
}
library("magrittr")
library("Rdrools")
library("dplyr")
library("purrr")
library("tibble")

## ----setup, include=FALSE------------------------------------------------
options(stringsAsFactors = F)

## ------------------------------------------------------------------------
data("iris")
data("irisRules")
sampleRules <- irisRules
rownames(sampleRules) <- seq(1:nrow(sampleRules))
sampleRules[is.na(sampleRules)]    <-""
sampleRules

## ---- warning=FALSE,message=FALSE, echo=FALSE----------------------------
#' Vignette helper functions
#' @description: Function plot graphs in the vignette
#' -----------------------------------------------------------------------------
#' @param result result of rule check
#' @param plotName Plot to be generated
#' @param rules the rules defined in csv format
#' -----------------------------------------------------------------------------
#' @return a plotly plot
#' @keywords internal

plotgraphs <- function(result,plotName){
  
  if(plotName == "Plot of points distribution"){
    anomaliesCountPlot <-list()
    purrr::map (1:length(result),function(i){
      outputDataframe <- result[[i]][["output"]]
      noOfTrueFalse <-  outputDataframe %>% dplyr::group_by(IsTrue) %>%
        dplyr::summarise(Frequency = n())
      if(nrow(noOfTrueFalse)==2){
        
        noOfTrueFalse <- noOfTrueFalse %>% as.data.frame %>% `rownames<-`(c("Anomalies","Non-Anomalies"))  
        anomaliesCountPlot[[i]] <- ggplot2::ggplot(noOfTrueFalse, ggplot2::aes(x=IsTrue, y=Frequency)) +
          ggplot2::geom_bar(stat = "identity", fill="steelblue")+
          ggplot2::labs(title="Distribution of points \n for the rule", 
              y = "Count") +
          ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1))
        
      }else{
        anomaliesCountPlot[[i]] <- NULL
      }
      
      return(anomaliesCountPlot)     
    })
  }else if(plotName == "Plot of groups"){
    plotAnomalies <-list()
    purrr::map (1:length(result),function(ruleNum){
      ruleName <- paste0("Rule",ruleNum)
      ruleValue <- paste0("Rule",ruleNum,"Value")
      intermediateOutput<- result[[ruleNum]][["intermediateOutput"]]
      
      if(class(intermediateOutput)=="list"){
        plotAnomalies[[ruleNum]] <- NULL
        
      }else {
        intermediateOutput<- dplyr::filter_(intermediateOutput,paste(ruleName,"==","'true'"))
        
        GroupedCols <- paste(colnames(intermediateOutput[,
                                                         !names(intermediateOutput) %in% c(ruleName,ruleValue)]),collapse = ":")
        intermediateOutput$Group <-  apply( intermediateOutput[ , !names(intermediateOutput) %in% c(ruleName,ruleValue) ] , 1 , paste , collapse = ":" )
        colnames(intermediateOutput)[ncol(intermediateOutput)-1] <- "values"
        
        plotAnomalies[[ruleNum]] <- ggplot2::ggplot(intermediateOutput, ggplot2::aes(x=Group, y=values))+
          ggplot2::geom_bar(stat = "identity",fill="steelblue")+
          ggplot2::labs(title="Groups satisfying the rule", 
               x=list(title = paste0("Grouped By - ",GroupedCols), tickangle = -45), y = "Aggregated Value") +
          ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1))
        
        return(plotAnomalies)
        
      }
    })
  }
}

