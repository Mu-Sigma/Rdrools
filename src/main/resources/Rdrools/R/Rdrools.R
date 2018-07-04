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

#' -----------------------------------------------------------------------------
#' @description: This function is used to convert the rules data uploaded into 
#'               required format
#' -----------------------------------------------------------------------------
#' @param dataset rules defined in a csv file
#' @param rules dataframe 
#' -----------------------------------------------------------------------------
#' @return rules in drl format, output dataframe 
#'
executeRulesOnDataset <- function(dataset, rules){
  if(nrow(dataset) == 0){
    stop("Dataset cannot be empty")
  }
  if(nrow(rules) == 0){
    stop("RulesData cannot be empty")
  }
  if(colnames(rules)[1] != "Filters" | colnames(rules)[2] != "GroupBy" |
     colnames(rules)[3] != "Column" | colnames(rules)[4] != "Function" |
     colnames(rules)[5] != "Operation" | colnames(rules) [6]!= "Argument"){
    stop("Column names of the rules file must be in the correct order")
  }
  
  rules[is.na(rules)] <- ""
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
          inputData <- filteredDataTrue
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
        inputData <- filteredDataTrue
        drlRules <- ruleToCompareColumns(dataset = dataset, rules = rules , 
                                         ruleNum = ruleNum)
        rules.Session <- rulesSessionDrl(unlist(drlRules), input.columns, 
                                         output.columns)
        outputDf %>% 
          append(., list(runRulesDrl(rules.Session, inputData))) -> outputDf
      }else{
        inputData <- filteredDataTrue
        rules.Session <- rulesSessionDrl(unlist(drlRules), input.columns,
                                         output.columns)
        outputDf %>% 
          append(., list(runRulesDrl(rules.Session, inputData))) -> outputDf
      }
    }else{
      inputData <- dataset
      filteredDataFalse <- NULL
      if(aggregationFunc=="compare"){
        drlRules <- ruleToCompareColumns(dataset = dataset, rules = rules , 
                                         ruleNum = ruleNum)
      }else if(aggregationFunc==""){
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
  
  # adding row numbers to the dataframe
  dataset$rowNumber <- 1:nrow(dataset)
  rules <-changecolnamesInRules(dataset = dataset, rules = rules )
  # converting factors to character
  rules[] <- lapply(rules, as.character)
  # getting input and output columns
  input.columns <- getrequiredColumns(dataset,rules)[[1]]
  output.columns <- getrequiredColumns(dataset,rules)[[2]]
  # changing the column names of the dataset by removing . or _ present in the column names
  colnames(dataset) <- input.columns
  rulesList <- list()
  outputCols <- list()
  # getting the required format to display output columns
  outputCols <-  map(colnames(dataset),function(x)paste0("output.put('",x, "',input.get('",x,"'));"))
  # defining lists which can be used later on when the function, getRuleInDrl
  #is called
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


#' -----------------------------------------------------------------------------
#' @description: This function is used to create the drools session for rules 
#'that are in drl format
#' -----------------------------------------------------------------------------
#' @param rules rules defined in a csv file
#' @param input.columns input columns of the dataframe
#' @param output.columns required output columns
#' -----------------------------------------------------------------------------
#' @return drools session
#' 
rulesSessionDrl <- function(rules, input.columns, output.columns) {
  rules <- paste(rules, collapse='\n')
  input.columns <- paste(input.columns,collapse=',')
  output.columns <- paste(output.columns,collapse=',')
  droolsSession<-.jnew('org/math/r/drools/DroolsService',rules,input.columns,
                       output.columns)
  return(droolsSession)
}



#' -----------------------------------------------------------------------------
#' @description: This function is used to execute rules on the dataframe and get
#' output 
#' -----------------------------------------------------------------------------
#' @param rules.session output of rulesSessionDrl function
#' @param input.df dataframe on which rules are to be run
#' -----------------------------------------------------------------------------
#' @return output dataframe
#'

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
  groupByCondition <- list()
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
#' @param dataset the dataframe on which rules are to be run
#' @param rules rules defined in a csv file
#' -----------------------------------------------------------------------------
#' @return required input columns and output columns
#' 
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
#' @param dataset the dataframe on which rules are to be run
#' @param rules rules defined in a csv file 
#' -----------------------------------------------------------------------------
#' @return rules in required format
#' 

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
#' @param dataset dataframe on which rules are to be run
#' @param rules rules in csv format
#' @param ruleNum the number of the current rule 
#' @param outputCols the output statments to show the output
#' @param input.columns input columns which are obtained from getrequiredColumns
#' @param output.columns output columns which are obtained from 
#' getrequiredColumns
#' -----------------------------------------------------------------------------
#' @return filtered output with flags true/false
#' 

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
#' @param dataset dataframe on which rules are to be run
#' @param rules the rules defined in csv format
#' @param ruleNum number of the rule which has condition to compare columns
#' -----------------------------------------------------------------------------
#' @return output in required format
#'

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
#' @param outputDf the output dataframe returned by the executeRulesOnDataset 
#' function
#' @param rules the rules defined in csv format
#' -----------------------------------------------------------------------------
#' @return output in required format
#'

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
      #setting all the rows of group as true/false according to the last row of each group
      #adding the filtered out rows with flag False to the final output
      outputDfForEachRule$Group<- apply( outputDfForEachRule[ , groupByColumn ] ,
                                         1 , paste , collapse = ":" )
      outputDfForEachRule <- outputDfForEachRule[, c("Group",ruleName,"Indices",
                                                     groupByColumn)]
      # getting the Indices of each group
      outputDfForEachRule[j,"Indices"]<-paste(seq(as.numeric(lowerRange), 
                                                  as.numeric(upperRange)),
                                              collapse = ",")
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
#' @description: This function is used to get the drl format of rules for row wise rules
#'               
#' -----------------------------------------------------------------------------
#' @param dataset rules defined in a csv file
#' @param outputDf the output dataframe returned by the executeRulesOnDataset function
#' @param rules the rules defined in csv format
#' -----------------------------------------------------------------------------
#' @return drl format of rules for row wise rules
#' 

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


