# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

.onLoad <- function(libname, pkgname) {
  .jpackage(pkgname, lib.loc = libname)
}

#'@name executeRulesOnDataset
#'@aliases executeRulesOnDataset
#'@title Run a set of rules on a dataset
#'@description The \emph{executeRulesOnDataset} function is an intuitive interface to execute rules on 
#'datasets, which is explicitly designed for data scientists. As input to this function rules are defined using the typical language of data science with verbs such as filter, group by and aggregate. Rules can be specified in a .csv file, loaded into the R session and passed to this function
#'@usage executeRulesOnDataset(dataset, rules)
#'@param dataset a data frame on which the defined set of rules should be applied
#'@param rules a data frame in which rules are defined
#'@details The vignette provides further details on the format of the rules file
#'@return  A list of input, intermediate output and output per rule
#'   \enumerate{
#'   \item \strong{input}: a tibble containing the rules defined by the user
#'   \item \strong{intermediateOutput}: is an empty tibble when there is no group by condition specified, 
#'   and a tibble with the aggregated value for each group when it is
#'   \item \strong{output}: a tibble with 3 columns:
#'     \enumerate{
#'     \item \strong{Group}: represents group name when there is a group by condition specified and the 
#'     row number in the case that there is no group by condition
#'     \item \strong{Indices}: the row numbers corresponding to each group when there is a group by 
#'     specified and row numbers of the data frame in the case that there is no group by condition
#'     \item \strong{IsTrue}: flag indicating if the data point/ set of data points satisfies the rule 
#'     or not. Returns TRUE if the point or group satisfies the rule and FALSE if not.  
#'     }
#'    }
#' @author Dheekshitha PS < Dheekshitha.PS@mu-sigma.com>
#' @author Naren Srinivasan <Naren.Srinivasan@mu-sigma.com>
#' @author Mayukh Bose <Mayukh.Bose@mu-sigma.com>
#' 
#' @seealso \code{\link{runRulesDrl}}, \code{\link{Rdrools}}
#' 
#' @examples
#'   library(Rdrools)
#'   data("iris")
#'   data("irisRules")
#'   executeRulesOnDataset(iris, irisRules)
#' @keywords rulesSessionDrl 
#' @keywords runRulesDrl 
executeRulesOnDataset <- function(dataset, rules){
  if(nrow(dataset) == 0){
    stop("Dataset cannot be empty")
  }
  if(nrow(rules) == 0){
    stop("RulesData cannot be empty")
  }
  if(colnames(rules)[1] != "Filters" | colnames(rules)[2] != "GroupBy" |
     colnames(rules)[3] != "Column" | colnames(rules)[4] != "Function" |
     colnames(rules)[5] != "Operation" | colnames(rules) [6] != "Argument"){
    stop("Column names of the rules file must be in the correct order")
  }
  
  rules[is.na(rules)] <- ""
  
#' @description Internal function to return the rule in DRL format
#' @keywords internal
  getRuleInDrl <- function(rowList){
    ruleNum <- rowList$ruleNum
    filterCond <- rowList$Filters
    groupByColumn  <- rowList$GroupBy
    aggregationColumn <- rowList$Column
    aggregationFunc <- rowList$Function
    operation <- rowList$Operation
    argument <- rowList$Argument
    ruleCondition <- NULL
    
    if(unlist(gregexpr(pattern = ',', groupByColumn)) != -1 &&
       groupByColumn != ""){
      # checking if there are more than one group by
      ruleCondition <- getConditionForMultiGroupBy(groupByColumn,
                                                   aggregationFunc, 
                                                   aggregationColumn)
    }else if(unlist(gregexpr(pattern = ',', groupByColumn)) == -1 &&
             groupByColumn == ""){
      # checking if there is no group by
      ruleCondition <-  paste0("result: Double()
                               from accumulate($condition:HashMap(),",
                               aggregationFunc, 
                               "(Double.valueOf($condition.get("
                               , shQuote(aggregationColumn),").toString())))")
    }else{
      #checking if there is only one group by
      groupByColumn <- groupByColumn
      groupbyCondition <- paste0(groupByColumn, ' == input.get("',
                                 groupByColumn, '")')
      ruleCondition <-    paste0("result: Double()
                                 from accumulate($condition:HashMap(", 
                                 groupbyCondition,"),",
                                 aggregationFunc,"(Double.valueOf($condition.get(",
                                 shQuote(aggregationColumn),").toString())))")
    }
    #generating drl format rule
    drlRules <-list(
      'import java.util.HashMap',
      'import java.lang.Double',
      'global java.util.HashMap output',
      "",
      '  dialect "mvel"',
      # Rules name
      paste0("rule \"Rule",ruleNum,"\""),
      '       salience 0',
      '       when',
      '        input: HashMap()',
      ruleCondition,
      'then'
    )  
    
    # Adding the condition for displaying output columns 
    drlRules <-  append(drlRules, outputCols)
    ruleValue <- paste0("Rule", ruleNum, "Value")
    drlRules %>% append(., paste0("output.put(\"Rule", ruleNum, "\",result", 
                                  operation, argument,");")) %>%
      append(., paste0("output.put(",shQuote(ruleValue),",result);")) %>%
      append(., 'end') -> drlRules
    rulesList %>% append(., list(drlRules)) %>% str(.) -> rulesList
    #storing input rule separately for each rule
    csvFormatOfEachRule %>% 
      append(., list(rowList %>% as_tibble())) -> csvFormatOfEachRule
    
    if(filterCond != ""){
      #when there is filter
      filteredData <- getDrlForFilterRules(dataset, rules, ruleNum, outputCols,
                                           input.columns, output.columns)
      ruleName <- paste0("Rule", ruleNum)
      filteredData <- filteredData[, c(input.columns, ruleName, ruleValue)]
      filteredData %>% 
        filter_(., paste(ruleName,"==","'true'")) -> filteredDataTrue
      filteredData %>% 
        filter_(., paste(ruleName,"==","'false'")) -> filteredDataFalse
      
      if(aggregationFunc == ""){
        if(operation != ""){
          
          drlRules <- getDrlForRowwiseRules(dataset, rules, ruleNum, 
                                            input.columns, output.columns)
          rules.Session <- rulesSessionDrl(unlist(drlRules), input.columns, 
                                           output.columns)
          outputDf %>% append(., list(filteredData)) -> outputDf
        }else{
          
          outputDf %>% append(., list(filteredData)) -> outputDf
          filteredDataFalse <- NULL
        }
      }else if(aggregationFunc == "compare"){
        drlRules <- ruleToCompareColumns(dataset = dataset, rules = rules, 
                                         ruleNum = ruleNum)
        rules.Session <- rulesSessionDrl(unlist(drlRules), input.columns, 
                                         output.columns)
        outputDf %>% 
          append(., list(runRulesDrl(rules.Session, filteredDataTrue))) -> outputDf
      }else{
        rules.Session <- rulesSessionDrl(unlist(drlRules), input.columns,
                                         output.columns)
        outputDf %>% 
          append(., list(runRulesDrl(rules.Session, filteredDataTrue))) -> outputDf
      }
    }else{
      
      filteredDataFalse <- NULL
      if(aggregationFunc=="compare"){
        drlRules <- ruleToCompareColumns(dataset = dataset, rules = rules , 
                                         ruleNum = ruleNum)
      }else if(aggregationFunc == ""){
        drlRules <-  getDrlForRowwiseRules(dataset, rules, ruleNum, 
                                           input.columns, output.columns)
      }
      rules.Session <- rulesSessionDrl(unlist(drlRules), input.columns,
                                       output.columns)
      outputDf %>% 
        append(., list(unique(runRulesDrl(rules.Session,dataset)))) -> outputDf
    }
    outputWithAllRows %>% 
      append(., list(formatOutput(dataset = dataset, 
                                  outputDf = outputDf[[length(outputDf)]],
                                  rules = rules,
                                  filteredDataFalse = filteredDataFalse,
                                  input.columns=input.columns, 
                                  ruleNum= ruleNum)$outputDf)) -> outputWithAllRows
    outputDfForEachRule %>% 
      append(., list(formatOutput(dataset = dataset, 
                                  outputDf = outputDf[[length(outputDf)]],
                                  rules = rules, 
                                  filteredDataFalse = filteredDataFalse,
                                  input.columns=input.columns,
                                  ruleNum = ruleNum)[[2]])) -> outputDfForEachRule
    
    intermediateOutput <- outputWithAllRows
    
    resultTibble = tibble(input = csvFormatOfEachRule,
                          intermediateOutput = intermediateOutput,
                          output=outputDfForEachRule)
    
    return(resultTibble)
  }
  
  # adding row numbers to the data frame
  dataset$rowNumber <- 1:nrow(dataset)
  rules <-changecolnamesInRules(dataset = dataset, rules = rules )
  # converting factors to character
  rules[] <- lapply(rules, as.character)
  # getting input and output columns
  input.columns <- getrequiredColumns(dataset,rules)[[1]]
  output.columns <- getrequiredColumns(dataset,rules)[[2]]
  # changing the column names of the dataset by removing . or _ present in the 
  # column names
  colnames(dataset) <- input.columns
  # getting the required format to display output columns
  rulesList <- list()
  outputCols <- list()
  outputCols <-  map(colnames(dataset),function(x)paste0("output.put('",x,
                                                         "',input.get('",x,
                                                         "'));"))
  # defining lists which can be used later on when the function, getRuleInDrl
  # is called
  
  csvFormatOfEachRule <- list()
  outputDf <- list()
  outputWithAllRows <- list()
  outputDfForEachRule <- list()
  # running the loop to get drl format for all the rules
  rules %>% mutate(ruleNum = 1:nrow(.)) -> rules
  rules %>% rowwise(.) %>% do(getRuleInDrl(.)) -> resultTib
  inp <- resultTib$input    
  interOut <- resultTib$intermediateOutput
  op <- resultTib$output
  
  output <- mapply(list,
                   input=inp,
                   intermediateOutput = interOut,
                   output=op, SIMPLIFY = F)
  
  return(output)
}

#'@name  rulesSessionDrl
#'@aliases rulesSessionDrl
#'@title Creates a session of the rules engine
#'@description The rulesSession creates a session that interfaces between R and the Drools engine. 
#'The session is utilized by the runRulesDrl function for executing a data frame against a set of rules
#'@usage rulesSessionDrl(rules, input.columns, output.columns)
#'@param rules a character vector consisting of lines read from a rules file of format \emph{.drl}
#'(Drools rules file). This character vector is eventually collapsed into a character vector of length 1, 
#'so the way you read the file could potentially be just about anything
#'@param  input.columns a character vector of a set of input column, for example. 
#'\code{input.columns<-c('name', 'class', 'grade', 'email')}
#'@param output.columns a character vector of a set of expected output columns, for example.
#' \code{output.columns<-c('address', 'subject', 'body')}
#'@details An active drools rules session. This promotes re-usability of a session, i.e. you can 
#'utilize the same session repetitively for different data sets of the same format.
#'@return rules.session.object Returns a session to the rules engine
#'@note Please have a look at the examples provided in the 'examples' section of the 
#'\code{\link{Rdrools}}. A sample data set and a set of rules have been supplied help you 
#'understand the package usage.
#'@author Ashwin Raaghav <ashwin.raaghav@mu-sigma.com>
#'@author SMS Chauhan <sms.chauhan@mu-sigma.com>
#'@seealso \code{\link{runRulesDrl}}, \code{\link{Rdrools}}
#'@examples
#' library(Rdrools)
#' data(class)
#' data(rules)
#' input.columns<-c('name', 'class', 'grade', 'email')
#' output.columns<-c('address', 'subject', 'body')
#' rulesSession<-rulesSessionDrl(rules, input.columns, output.columns)
#' output.df<-runRulesDrl(rulesSession, class)
#'@keywords runRulesDrl 
#'@keywords rulesSessionDrl 

rulesSessionDrl <- function(rules, input.columns, output.columns) {
  rules <- paste(rules, collapse='\n')
  input.columns <- paste(input.columns,collapse=',')
  output.columns <- paste(output.columns,collapse=',')
  droolsSession <- .jnew('org/math/r/drools/DroolsService',rules, input.columns,
                         output.columns)
  return(droolsSession)
}


#'@name runRulesDrl
#'@aliases runRulesDrl
#'@title Apply a set of rule transformations to a data frame
#'@description This function is the core of the Rdrools package. Rules are applied on an input data frame 
#'and the results are returned as the output of the function. The columns on which the rules need to be 
#'applied have to be provided explicitly. Additionally, the new columns that would be created based on
#' the rules have to be provided explicitly as well. The rules engine picks up a row from the data frame,
#'  applies the transformation to it based on rules provided and saves the result in an output data frame. 
#'@usage runRulesDrl(rules.session, input.df)
#'@param rules.session a session of the rules engine created using the \code{\link{rulesSessionDrl}} function
#'@param input.df a data frame consisting of a set of rows you wish to transform, and columns you 
#'wish to use in the transformation
#'@details If you are not familiar with the Drools file format, please have a look at the references 
#'provided in the \code{\link{Rdrools}}. More details on how conflicting rules are resolved 
#'using either salience or the Reete algorithm are also present in the references.
#'@return output.df a data frame which is the result of transformations applied to the input data 
#'frame(\code{input.df}), the columns being the list provided through the \code{output.columns} 
#'parameter in \code{\link{rulesSessionDrl}}
#'@details
#'\strong{Transformation policy} Transformations are applied row by row, iteratively. That is to say, 
#'all inputs required for a rule transformation should be present in columns as a part of that row 
#'itself. Each row should be considered #'independent of another; all input values required for a
#' transformation should be available in that row itself. The expectation from rules engines are 
#' often misplaced.
#'\strong{Column Mismatch} Please make sure that the list of output columns provided through the 
#'\code{output.columns} parameter is exhaustive. Any additional column which is created through the 
#'rules transformation but is not present in the list would inhibit proper functioning. In most cases,
#' an error should be thrown.
#'@seealso \code{\link{runRulesDrl}}, \code{\link{Rdrools}} 
#'@author Ashwin Raaghav <ashwin.raaghav@mu-sigma.com>
#'@author SMS Chauhan <sms.chauhan@mu-sigma.com>
#'@examples
#' library(Rdrools)
#' data(class)
#' data(rules)
#' input.columns<-c('name', 'class', 'grade', 'email')
#' output.columns<-c('address', 'subject', 'body')
#' rulesSession<-rulesSessionDrl(rules, input.columns, output.columns)
#' output.df<-runRulesDrl(rulesSession, class)
#'@keywords runRulesDrl 
#'@keywords rulesSessionDrl 

runRulesDrl<-function(rules.session,input.df) {
  conn <- textConnection('input.csv.string','w')
  write.csv(input.df,file=conn)
  close(conn)
  input.csv.string <- paste(input.csv.string, collapse='\n')
  output.csv.string <- .jcall(rules.session, 'S', 'execute',input.csv.string)
  conn <- textConnection(output.csv.string, 'r')
  output.df<-read.csv(file=conn, header=T)
  close(conn)
  return(output.df)
}


#### Helper functions ######

#' -----------------------------------------------------------------------------
#' @description: This function is used to get the required rule condition when 
#' there are multiple groupby columns
#' -----------------------------------------------------------------------------
#' @param groupByColumn columns to groupby
#' @param aggregationFunc aggregation function to be applied 
#' @param aggregationColumn column on which aggregation function is to be 
#' applied  
#' -----------------------------------------------------------------------------
#' @return required rule condition 
#'

getConditionForMultiGroupBy <- function(groupByColumn, aggregationFunc, 
                                        aggregationColumn){
  
  ruleCondition <-   map(groupByColumn,function(groupByColumn){
    #making groupby condition if there are multiple groupby
    groupByColumn <- unlist(strsplit(unlist(groupByColumn),","))
    groupByCondition <- paste0(groupByColumn,'==input.get("',groupByColumn,'")')
    groupByCondition <-paste0(groupByCondition,collapse = ",")
    ruleCondition <-    paste0("result: Double()
                               from accumulate($condition:HashMap(",
                               groupByCondition,"),",
                               aggregationFunc,
                               "(Double.valueOf($condition.get(",
                               shQuote(aggregationColumn),").toString())))")
    
    
  })
  
  return(ruleCondition)
}

#' -----------------------------------------------------------------------------
#' @description: This function is used to get the required input and output 
#' columns
#' -----------------------------------------------------------------------------
#' @param dataset the data frame on which rules are to be run
#' @param rules rules defined in a csv file
#' -----------------------------------------------------------------------------
#' @return required input columns and output columns
#' @keywords internal
getrequiredColumns <- function(dataset,rules){
  # removing the . or _ in the column names of the dataset
  colnames(dataset) <- gsub('\\.|\\_', '', colnames(dataset))
  input.columns <- colnames(dataset)
  output.columns <-list()
  valueColumns <- list()
  noOfColumns <- ncol(dataset)
  # adding one column for each rule
  output.columns <- lapply(1:nrow(rules), function(rulesRowNumber){
    output.columns[noOfColumns+rulesRowNumber] <- paste0("Rule", rulesRowNumber)
    
  })
  # adding columns to store the result value for each rule
  valueColumns <- lapply(1:nrow(rules), function(rulesRowNumber){
    valueColumns[rulesRowNumber] <- paste0("Rule", rulesRowNumber, "Value")
  })
  input.columns %>% append(.,list(output.columns)) %>% 
    append(.,list(valueColumns)) -> output.columns 
  
  return(list(input.columns=input.columns,
              output.columns=unlist(output.columns)))
}

#' -----------------------------------------------------------------------------
#' @description: This function is used to remove the . or _ in column names of
#'  the dataset present in the rules file
#' -----------------------------------------------------------------------------
#' @param dataset the data frame on which rules are to be run
#' @param rules rules defined in a csv file 
#' -----------------------------------------------------------------------------
#' @return rules in required format
#' @keywords internal

changecolnamesInRules <-function(dataset,rules){
  
  formattedRules <- rules
  for (col.name in colnames(dataset)) {
    formattedRules <- data.frame(
      lapply(formattedRules, function(x){
        #checking for the column names of dataset present in rules
        gsub(pattern = col.name, 
             replacement = gsub(pattern = "\\.|\\_",
                                replacement = "", col.name), x)}))
  }
  return(formattedRules)
  
}


#' -----------------------------------------------------------------------------
#' @description: This function is used to get the drl format for the rules 
#' which have filters
#' -----------------------------------------------------------------------------
#' @param dataset data frame on which rules are to be run
#' @param rules rules in csv format
#' @param ruleNum the number of the current rule 
#' @param outputCols the output statments to show the output
#' @param input.columns input columns which are obtained from getrequiredColumns
#' @param output.columns output columns which are obtained from 
#' getrequiredColumns
#' -----------------------------------------------------------------------------
#' @return filtered output with flags true/false
#' @keywords internal
getDrlForFilterRules <- function(dataset, rules, ruleNum, outputCols, 
                                 input.columns, output.columns){
  # this function is used to create rules in drl for the rules which 
  # involve filters. this is done by creating the complementary rule for the 
  # given filter and then labelling the true/false
  filterCond <- rules[ruleNum,"Filters"]
  #filter given
  condition <- paste0("input:HashMap(",filterCond,")")
  ruleValue <- paste0("Rule",ruleNum,"Value")
  #defining the complementary condition for the filter given
  compcondition <- paste0("input:HashMap(!(",filterCond,"))")
  outputforFilter <- paste0("output.put(\"Rule",ruleNum,
                            "\"",", \"\"+","\"true\"",");")
  
  outputforFilterComp <- paste0("output.put(\"Rule",
                                ruleNum,"\"",", \"\"+","\"false\"",");")
  
  drlRules <-  list("import java.util.HashMap;",
                    "global java.util.HashMap output;",
                    "",
                    paste0("rule \"Rule",ruleNum,"\""),
                    "\tsalience 0",#rule priority
                    "\twhen",
                    condition,
                    "\tthen")
  drlRules <- append(drlRules,outputCols)
  
  compDrl<-list(
    paste0("output.put('", ruleValue, "',", shQuote(filterCond), " );"),
    outputforFilter,
    "end",
    "",
    "rule \"rule fail\"",
    "\tsalience 1",
    "\twhen",
    compcondition,
    "\tthen")
  
  drlRules <- append(drlRules,compDrl)
  drlRules%>%append(.,outputCols)%>%append(.,list(outputforFilterComp))%>%
    append(.,paste0("output.put('",ruleValue,"',",
                    shQuote(paste("Not",filterCond)),");"))%>%
    append(.,'end') -> drlRules

  filteredOutputSession <- rulesSessionDrl(drlRules, input.columns, 
                                           output.columns=output.columns)
  filteredOutput <- runRulesDrl(filteredOutputSession, dataset)
  
  return(filteredOutput)
}

#' -----------------------------------------------------------------------------
#' @description: This function is used to get drl rule for rules involving
#'  comparing columns
#' -----------------------------------------------------------------------------
#' @param dataset data frame on which rules are to be run
#' @param rules the rules defined in csv format
#' @param ruleNum number of the rule which has condition to compare columns
#' -----------------------------------------------------------------------------
#' @return output in required format
#' @keywords internal

ruleToCompareColumns <- function(dataset, rules, ruleNum){
  
  aggregationColumn <- rules[ruleNum,"Column"]
  operation <-  rules[ruleNum,"Operation"]
  argument <-rules[ruleNum,"Argument"]
  compareCondition <- paste0('result:HashMap(Double.valueOf(this["',
                             aggregationColumn,'"]) ', 
                             operation,
                             ' Double.valueOf(this["',argument,'"]))')
  drlRules <-list(
    'import java.util.HashMap',
    'import java.lang.Double',
    'global java.util.HashMap output',
    "",
    '  dialect "mvel"',
    # rule's name
    paste0("rule \"Rule",ruleNum,"\""),
    '       salience 0',
    '       when',
    '        input: HashMap()',
    compareCondition,
    'then'
  )
  outputCols <-  map(colnames(dataset),function(x)paste0("output.put('",x, "',
                                                         result.get('",x,"'));"))
  
  drlRules <-  append(drlRules, outputCols)
  ruleValue <- paste0("Rule", ruleNum, "Value")  
  drlRules%>%append(., paste0("output.put(\"Rule", 
                              ruleNum,"\"",", \"\"+","\"true\"",");"))%>%
    append(.,paste0("output.put(",shQuote(ruleValue),",result);")  )%>%
    append(.,'end') -> drlRules
  
  return(drlRules)
  
}

#' -----------------------------------------------------------------------------
#' @description: This function is used to change the output of the rules 
#' involving groupby
#' -----------------------------------------------------------------------------
#' @param dataset rules defined in a csv file
#' @param outputDf the output data frame returned by the executeRulesOnDataset 
#' function
#' @param rules the rules defined in csv format
#' @param filteredDataFalse the fitlered data which is flagged as false
#' @param input.columns the input columns of the dataset which is obtained
#' from getrequiredColumns function
#' @param ruleNum the number of the rule currently being run
#' -----------------------------------------------------------------------------
#' @return output in required format
#' @keywords internal

formatOutput <- function(dataset, outputDf, rules, filteredDataFalse, 
                         input.columns, ruleNum){
  # adding row number as a column to identify the last and first row for each group
  groupByColumn  <- rules[ruleNum,"GroupBy"]
  aggregationFunc <-noquote( rules[ruleNum,"Function"])
  ruleName <- paste0("Rule",ruleNum)
  ruleValue <- paste0("Rule",ruleNum,"Value")
  
  if(groupByColumn!=""){
    
    #getting the first and last row for each group
    outputFormatted <-  eval(parse(text=paste('outputDf%>%group_by(',
                                              groupByColumn,')%>%
                                              slice(c(1,n()))%>%ungroup()')))  
    #getting the last row for each group
    outputDfForEachRule <- eval(parse(text=paste('outputDf%>%
                                                 group_by(', groupByColumn, ')%>%
                                                 slice(c(n()))%>%ungroup()')))
    #adding new column, Indices
    groupByColumn <-unlist(strsplit(groupByColumn,","))
    outputDf<- outputDfForEachRule[,c(groupByColumn,ruleName,ruleValue)]
    outputDfForEachRule$Indices <- 0
    for(j in 1:nrow(outputDfForEachRule)){
      lowerRange<-outputFormatted[(2*j)-1,"rowNumber"]
      upperRange <- outputFormatted[(2*j),"rowNumber"]
      # setting all the rows of group as true/false according to the 
      # last row of each group
      # adding the filtered out rows with flag False to the final output
      outputDfForEachRule$Group<- apply( outputDfForEachRule[ , groupByColumn ] ,
                                         1 , paste , collapse = ":" )
      outputDfForEachRule <- outputDfForEachRule[, c("Group",ruleName,"Indices",
                                                     groupByColumn)]
      # getting the Indices of each group
      outputDfForEachRule[j,"Indices"]<-paste(seq(as.numeric(lowerRange), as.numeric(upperRange)),collapse = ",")
      outputDfForEachRule <- outputDfForEachRule[,c("Group","Indices",
                                                    ruleName, groupByColumn)]
    }   }else{
      
      if(aggregationFunc != "compare" && aggregationFunc != ""){
        # agg on whole column  
        # getting the last row
        outputFormatted <- as_tibble(outputDf)%>%slice(n())
        
        ifelse(outputFormatted[,ruleName][1,]=='true',
               outputDf[c(1:nrow(outputDf)), ruleName] <-"true",
               outputDf[c(1:nrow(outputDf)), ruleName]  <- "false")
        # getting the required columns
        outputDf <- outputDf[,c(input.columns,ruleName,ruleValue)]
        outputDf <- rbind(outputDf,filteredDataFalse)
        outputDfForEachRule <- outputFormatted
        outputDfForEachRule$Group <- 1
        outputDfForEachRule$Indices <- paste(outputDf[, "rowNumber"],
                                             collapse = ",")
        outputDfForEachRule <- outputDfForEachRule[,c("Group", "Indices",
                                                      ruleName)]
        outputDf <- list()
      }else if(aggregationFunc=="compare"){
        
        outputDf <- outputDf[,c(input.columns,ruleName,ruleValue)]
        outputDf <- rbind(outputDf,filteredDataFalse)
        
        outputDfForEachRule <- outputDf
        rowsFailedInComparision <- dataset[-(outputDf$rowNumber),]
        ruleName <- paste0("Rule",ruleNum)
        ruleValue <- paste0("Rule",ruleNum,"Value")
        if(nrow(rowsFailedInComparision)!=0){
          
          rowsFailedInComparision[,ruleName] <-"false"
          rowsFailedInComparision[,ruleValue] <-"NA"
          outputDfForEachRule <- rbind(outputDfForEachRule, 
                                       rowsFailedInComparision)
        }
        outputDfForEachRule$Group <- outputDfForEachRule$rowNumber
        outputDfForEachRule$Indices <- outputDfForEachRule$rowNumber
        outputDfForEachRule <- outputDfForEachRule[,c("Group","Indices",
                                                      ruleName)]
        outputDf <- list()
      }else{
        outputDfForEachRule <- outputDf
        outputDfForEachRule$Group <-outputDfForEachRule$rowNumber
        outputDfForEachRule$Indices <- outputDfForEachRule$rowNumber
        outputDfForEachRule <- outputDfForEachRule[,c("Group","Indices",
                                                      ruleName)]
        outputDf <- list()
      }
      
    }
  outputDfForEachRule <- outputDfForEachRule[,c("Group","Indices",ruleName)]
  outputDfForEachRule <- setNames(outputDfForEachRule,
                                  c("Group","Indices","IsTrue"))
  
  return(list(outputDf=outputDf,outputDfForEachRule=outputDfForEachRule))
}


#' -----------------------------------------------------------------------------
#' @description: This function is used to get the drl format of rules for row 
#' wise rules
#' -----------------------------------------------------------------------------
#' @param dataset rules defined in a csv file
#' @param outputDf the output data frame returned by the executeRulesOnDataset 
#' function
#' @param rules the rules defined in csv format
#' -----------------------------------------------------------------------------
#' @return drl format of rules for row wise rules
#' @keywords internal

getDrlForRowwiseRules <- function(dataset, rules, ruleNum, input.columns, 
                                  output.columns){
  
  aggregationColumn <- rules[ruleNum,"Column"]
  operation <-  rules[ruleNum,"Operation"]
  argument <- rules[ruleNum,"Argument"]
  outputCols <-  map(colnames(dataset),function(x)paste0("output.put('",
                                                         x, "',
                                                         result.get('",x,"'));"))
  
  inCondition <- paste0("result:HashMap(this[\"", aggregationColumn, "\"]  ",
                        operation," (", argument, "))")
  
  drlRules <-  list("import java.util.HashMap;",
                    "global java.util.HashMap output;",
                    "",
                    paste0("rule \"Rule",ruleNum,"\""),
                    "\tsalience 0",#rule priority
                    "\twhen",
                    inCondition,
                    "\tthen")
  drlRules <- append(drlRules,outputCols)
  drlRules%>%append(.,paste0("output.put(\"Rule",ruleNum,
                             "\"",", \"\"+","\"true\"",");"))%>%
    append(.,'end')->drlRules
  return(drlRules)  
}
