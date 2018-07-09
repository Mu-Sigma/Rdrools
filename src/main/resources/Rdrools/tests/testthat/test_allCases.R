################################################################################################################
# Title : Test cases for Rdrools
# Created : 4 July 2018
# Author : Dheekshitha PS, Naren Srinivasan
################################################################################################################

context("Test execution of rules")
library(Rdrools)
data("transactionData")
data("transactionRules")

    
  test_that("Empty Datasets are not allowed for running rules", {
    expect_error(executeRulesOnDataset(data.frame(Date=as.Date(character()),
                                                  File=character(), 
                                                  User=character(), 
                                                  stringsAsFactors=FALSE) ,transactionRules),
                 "Dataset cannot be empty")
  })
  
  test_that("RulesData cannot be empty", {
    expect_error(executeRulesOnDataset(transactionData,data.frame(Filters=character(),
                                                                GroupBy=character(), 
                                                                Column=character(),Function=character(),
                                                                Operation=character(),Argument=character(),
                                                                stringsAsFactors=FALSE)),
                 "RulesData cannot be empty")
  })
  
  test_that("Column names of the rules file should be in predefined order", {
    expect_error(executeRulesOnDataset(transactionData,wrongRules),
                 "Column names of the rules file must be in the correct order")
  })
