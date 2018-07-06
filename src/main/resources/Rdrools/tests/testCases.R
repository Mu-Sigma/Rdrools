################################################################################################################
# Title : Test cases for Rdrools
# Created : 4 July 2018
# Author : Dheekshitha PS
################################################################################################################


if(!require(testthat)){
  install.packages("testthat")
}


library(testthat)

sampleDataset <- data("transactionData")
wrongRules <- data("wrongRules")
sampleRules <- data("transactionRules")

test <- function(){
  failedTests <-  list()
  error <- tryCatch({
    
    emptyDataset <- data.frame(Date=as.Date(character()),
                               File=character(), 
                               User=character(), 
                               stringsAsFactors=FALSE) 
    
    test_that("Empty Datasets are not allowed for running rules", {
      expect_error(executeRulesOnDataset(emptyDataset,sampleRules),
                   "Dataset cannot be empty")
    })
  },
  error = function(e){return(e)})
  
  if(!isTRUE(error)){
    failedTests[length(failedTests)+1] <- as.character(error)
  }
  error <- tryCatch({
    
    emptyRules <- data.frame(Filters=character(),
                             GroupBy=character(), 
                             Column=character(),Function=character(),
                             Operation=character(),Argument=character(),
                             stringsAsFactors=FALSE)   
    
    test_that("RulesData cannot be empty", {
      expect_error(executeRulesOnDataset(sampleDataset,emptyRules),
                   "RulesData cannot be empty")
    })
  },
  error = function(e){return(e)})
  
  if(!isTRUE(error)){
    failedTests[length(failedTests)+1] <- as.character(error)
  }
  error <- tryCatch({
    
    
    test_that("Column names of the rules file should be in predefined order", {
      expect_error(executeRulesOnDataset(sampleDataset,wrongRules),
                   "Column names of the rules file must be in the correct order")
    })
  },
  error = function(e){return(e)})
  
  if(!isTRUE(error)){
    failedTests[length(failedTests)+1] <- as.character(error)
  }  
  
  
  
  if(length(failedTests) == 0){
    return("All Tests Passed")
  }
  else{
    return(failedTests)
  }
  
}
test()
