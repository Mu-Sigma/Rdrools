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
#' @description: This function is used to convert the rules data uploaded into required format
#'               
#' -----------------------------------------------------------------------------
#' @param dataset rules defined in a csv file
#' @param rules dataframe 
#' -----------------------------------------------------------------------------
#' @return rules in drl format, output dataframe 
#' 
executeRulesOnDataset <- function(dataset,rules){
  
  rulesList <- list()
  outputCols <- list()
  # Getting the required format to display output columns
  outputCols <-  map(colnames(dataset),function(x)paste0("output.put('",x, "',input.get('",x,"'));"))
  perRule <- list()
  outputDf <- list()
  outputDfForEachRule <- list()
  input.columns <- getrequiredColumns(dataset,rules)[[1]]
  output.columns <- getrequiredColumns(dataset,rules)[[2]]
  
  # Running the loop to get drl format for all the rules
  
  for(i in 1:nrow(rules)){
    filterData <- rules[i,"Filters"]
    groupbyColumn  <- rules[i,"GroupBy"]
    aggregateCoulmn <- rules[i,"Column"]
    aggregationFunc <-noquote( rules[i,"Function"])
    operation <-  rules[i,"Operation"]
    argument <- rules[i,"Argument"]
    
    
    # checking if there are more than one group by
    if(unlist(gregexpr(pattern =',',groupbyColumn))!=-1 && groupbyColumn!=""){
      n <- length(unlist(gregexpr(pattern =',',groupbyColumn)))
      groupbyCondition <- list()
      accumulateCondition <-   map(groupbyColumn,function(groupbyColumn){
        #making groupby condition if there are multiple groupby
        groupbyColumn <- unlist(strsplit(unlist(groupbyColumn),","))
        groupbyCondition <- paste0(groupbyColumn,'==input.get("',groupbyColumn,'")')
        groupbyCondition <-paste0(groupbyCondition,collapse = ",")
        
        
        #condition when there is no filter
        
        accumulateCondition <-    paste0("result: Double()
                                         from accumulate($condition:HashMap(",groupbyCondition,"),",
                                         aggregationFunc,"(Double.valueOf($condition.get(",shQuote(aggregateCoulmn),").toString())))") 
        
        
      })
    }else if(unlist(gregexpr(pattern =',',groupbyColumn))==-1 &&groupbyColumn=="" ){#No groupby  
      
      if(aggregationFunc=="compare"){
        #Rules having compare and no filter
        
        accumulateCondition <- paste0('result:HashMap(Double.valueOf(this["',aggregateCoulmn,'"]) ',operation,' Double.valueOf(this["',argument,'"]))')
        
      }else if(aggregationFunc!=""){
        #Rules having no compare and no filter i.e. aggregation on a column
        
        accumulateCondition <-  paste0("result: Double()
                                       from accumulate($condition:HashMap(),",
                                       aggregationFunc,"(Double.valueOf($condition.get(",shQuote(aggregateCoulmn),").toString())))")
      }else{#only filter (i.e aggregationfunc is empty)
        accumulateCondition <- NULL
      }
    }else{
      
      groupbyColumn <- groupbyColumn
      groupbyCondition <- paste0(groupbyColumn,'==input.get("',groupbyColumn,'")')
      
      
      accumulateCondition <-    paste0("result: Double()
                                       from accumulate($condition:HashMap(",groupbyCondition,"),",
                                       aggregationFunc,"(Double.valueOf($condition.get(",shQuote(aggregateCoulmn),").toString())))")
      
    }
    
    drlRules <-list(
      'import java.util.HashMap',
      'import java.lang.Double',
      'global java.util.HashMap output',
      "",
      '  dialect "mvel"',
      # Rules name
      paste0("rule \"Rule",i,"\""),
      '       salience 0',
      '       when',
      '        input: HashMap()',
      #filtering
      accumulateCondition,
      'then'
    )    
    #adding the condition for displaying output columns 
    drlRules <-  append(drlRules, outputCols)
    
    if(aggregationFunc=="compare"){
      drlRules[length(drlRules)+1] <- paste0("output.put(\"Rule",i,"\",'",aggregateCoulmn,operation,argument,"');")  
    }else{
      drlRules[length(drlRules)+1] <- paste0("output.put(\"Rule",i,"\",result",operation,argument,");")      
    }
    
    drlRules[length(drlRules)+1] <-'end'
    rulesList[[i]] <- drlRules
    perRule[[i]] <- list(rules[i,],drlRules)
    
    
    if(filterData != ""){
      
      filteredData <- getDrlForFilterRules(dataset,rules,i,outputCols,input.columns,output.columns)
      ruleName <- paste0("Rule",i)
      
      filteredData <- filteredData[,c(input.columns,ruleName)]
      filteredDataTrue <-filter_(filteredData,paste(ruleName,"==","'true'"))
      filteredDataFalse <-filter_(filteredData,paste(ruleName,"==","'false'"))
      
      if(aggregationFunc==""){
        outputDf[[i]] <- filteredData
        filteredDataFalse <- NULL
      }else{
        inputData <- filteredDataTrue
        rules.Session <- rulesSession(unlist(drlRules),input.columns,output.columns)
        outputDf[[i]] <- runRules(rules.Session,inputData)  
      }
      
    }else{
      inputData <- dataset
      filteredDataFalse <- NULL
      rules.Session <- rulesSession(unlist(drlRules),input.columns,output.columns)
      outputDf[[i]] <- runRules(rules.Session,inputData)  
    }
    
    outputDfForEachRule[[i]] <- formatOutput(dataset = dataset,outputDf = outputDf[[i]],rules = rules,filteredDataFalse =filteredDataFalse ,input.columns=input.columns,ruleNum=i)
    
  }
  return(list(perRule=perRule,outputDfForEachRule=outputDfForEachRule))
  
}

#' -----------------------------------------------------------------------------
#' @description: This function is used to get the required input and output columns
#'               
#' -----------------------------------------------------------------------------
#' @param dataset rules defined in a csv file
#' @param rules dataframe 
#' -----------------------------------------------------------------------------
#' @return required input columns and output columns
#' 
getrequiredColumns <- function(dataset,rules){
  input.columns <- colnames(dataset)
  output.columns <-list()
  j<- ncol(dataset)
  #adding each column for each rule
  output.columns<-lapply(1:nrow(rules), function(i){
    output.columns[j+i] <- paste0("Rule",i)
    
  })
  output.columns <- unlist(append(input.columns,output.columns))
  return(list(input.columns=input.columns,output.columns=output.columns))
}



#' -----------------------------------------------------------------------------
#' @description: This function is used to get the drl format for the rules which have only filters
#'               
#' -----------------------------------------------------------------------------
#' @param dataset input dataset
#' @param rules rules in csv format
#' @param ruleNum the current rule number
#' @param outputCols the output statments to show the output
#' @param input.columns input columns
#' @param output.columns output columns
#' -----------------------------------------------------------------------------
#' @return filtered output with flags true/false
#' 


getDrlForFilterRules <- function(dataset,rules,ruleNum,outputCols,input.columns,output.columns){
  #this fucntion is used to create rules in drl for the rules which involve only filters
  #this is done by creating the complementary rule for the given filter and then labelling the true/false
  filterData <- rules[ruleNum,"Filters"]
  condition <- paste0("input:HashMap(",filterData,")")
  
  compcondition <- paste0("input:HashMap(!(",filterData,"))")
  outputforFilter <- paste0("output.put(\"Rule",ruleNum,"\"",", \"\"+","\"true\"",");")
  
  outputforFilterComp <- paste0("output.put(\"Rule",ruleNum,"\"",", \"\"+","\"false\"",");")
  
  drlRules <-  list("import java.util.HashMap;",
                    "global java.util.HashMap output;",
                    "",
                    paste0("rule \"Rule",ruleNum,"\""),
                    "\tsalience 0",#rule priority
                    "\twhen",
                    condition,
                    "\tthen")
  drlRules <-append(drlRules,outputCols)
  
  compDrl<-list(
    outputforFilter,
    "end",
    "",
    "rule \"rule fail\"",
    "\tsalience 1",
    "\twhen",
    compcondition, 
    
    "\tthen")
  
  drlRules <- append(drlRules,compDrl)
  drlRules<-append(drlRules,outputCols)
  drlRules[length(drlRules)+1] <- outputforFilterComp
  drlRules[length(drlRules)+1] <-'end'
  filteredOutputSession <- rulesSession(drlRules,input.columns,output.columns=output.columns)
  filteredOutput <- runRules(filteredOutputSession,dataset)
  
  return(filteredOutput)
}




#' -----------------------------------------------------------------------------
#' @description: This function is used to change the output of the rules involving groupby
#'               
#' -----------------------------------------------------------------------------
#' @param dataset rules defined in a csv file
#' @param outputDf the output dataframe returned by the executeRulesOnDataset function
#' @param rules the rules defined in csv format
#' -----------------------------------------------------------------------------
#' @return output in required format
#' 
formatOutput <- function(dataset,outputDf,rules,filteredDataFalse,input.columns,ruleNum){
  #addign row number as a column to identify the last and first row for each group
  outputDf$rowNumber <- 1:nrow(outputDf)
  # for(i in 1:nrow(rules)){
  groupbyColumn  <- rules[ruleNum,"GroupBy"]
  aggregationFunc <-noquote( rules[ruleNum,"Function"])
  if(groupbyColumn!=""){
    ruleName <- paste0("Rule",ruleNum)
    #getting the first and last row for each group
    
    
    outputFormatted <-  eval(parse(text=paste('outputDf%>%group_by(',groupbyColumn,')%>%slice(c(1,n()))%>%ungroup()')))  
    
    for(j in 1:nrow(outputFormatted)){
      lowerRange<-outputFormatted[2*j-1,"rowNumber"]
      upperRange <- outputFormatted[2*j,"rowNumber"]
      #setting all the rows of group as true/false according to the last row of each group
      ifelse(outputFormatted[,ruleName][2*j,]=='true',outputDf[c(as.numeric(lowerRange):as.numeric(upperRange)), ruleName] <-"true",outputDf[c(as.numeric(lowerRange):as.numeric(upperRange)), ruleName]  <- "false")
      outputDf <- outputDf[,c(input.columns,ruleName)]
      outputDf <- rbind(outputDf,filteredDataFalse)
    }  
  }else{
    ruleName <- paste0("Rule",ruleNum)
    
    if(aggregationFunc != "compare" && aggregationFunc != ""){
      
      outputFormatted <- outputDf%>%slice(n())
      ifelse(outputFormatted[,ruleName][1,]=='true',outputDf[c(1:nrow(outputDf)), ruleName] <-"true",outputDf[c(1:nrow(outputDf)), ruleName]  <- "false")
      
      outputDf <- outputDf[,c(input.columns,ruleName)]
      outputDf <- rbind(outputDf,filteredDataFalse)
    }else{
      ruleName <- paste0("Rule",ruleNum)
      outputDf <- outputDf[,c(input.columns,ruleName)]
      outputDf <- rbind(outputDf,filteredDataFalse)
    }
    
  }
  
  
  
  return(outputDf)
}



#' -----------------------------------------------------------------------------
#' @description: This function ris used to get the rule wise output
#'               
#' -----------------------------------------------------------------------------
#' @param dataset rules defined in a csv file
#' @param outputDf output returned by the executeRulesOnDataset function
#' @param rules rules in csv format 
#' -----------------------------------------------------------------------------
#' @return rule wise output
#' 

getRuleWiseData <-function(dataset,outputDf,rules,ruleNum){
 # outputForAllRules <-list()
  #outputForAllRules <- map(1:nrow(rules),function(i){
    groupbyColumn  <- rules[ruleNum,"GroupBy"]
    ruleName <- paste0("Rule",ruleNum)
    if(groupbyColumn!=""){
      
      #getting the required rows from the output
      metaDataperRule <- eval(parse(text=paste('outputDf%>%group_by(',groupbyColumn,')%>%slice(c(n()))%>%ungroup()')))
      #getting the required ciolumns from the output
      groupbyColumn <-unlist(strsplit(groupbyColumn,","))
      metaDataperRule <- metaDataperRule[,c(groupbyColumn,ruleName)]
    }else{
      #getting the required colums from the output
      metaDataperRule <- outputDf
    }
    #outputForAllRules[[i]] <- outputPerRule
    

  return(metaDataperRule)
}
#' -----------------------------------------------------------------------------
#' @description: This function is used to call the drools session for rules that are in drl format
#'               
#' -----------------------------------------------------------------------------
#' @param dataset rules defined in a csv file
#' @param input.columns input columns of the dataframe
#' @param output.columns required output columns
#' -----------------------------------------------------------------------------
#' @return drools session
#' 
rulesSessionDrl <- function(rules,input.columns, output.columns) {
  rules <- paste(rules, collapse='\n')
  input.columns <- paste(input.columns,collapse=',')
  output.columns <- paste(output.columns,collapse=',')
  droolsSession<-.jnew('org/math/r/drools/DroolsService',rules,input.columns, output.columns)
  return(droolsSession)
}

#' -----------------------------------------------------------------------------
#' @description: This function is used to call the drools session for rules that are in decision table format
#'               
#' -----------------------------------------------------------------------------
#' @param rulesDT rules defined in a decision table
#' @param input.columns input columns of the dataframe
#' @param output.columns required output columns
#' -----------------------------------------------------------------------------
#' @return drools session
#' 
rulesSessionDT<-function(rulesDT,input.columns,output.columns){
  
  
  
  
  
  
}


#' -----------------------------------------------------------------------------
#' @description: This function is used to execute all kinds of rules (defined in csv or drl or DT format)
#'               
#' -----------------------------------------------------------------------------
#' @param rules.session output fo rulesSession function
#' @param input.df input dataframe
#' -----------------------------------------------------------------------------
#' @return output dataframe
#'

runRulesDrl<-function(rules.session,input.df) {
  conn<-textConnection('input.csv.string','w')
  write.csv(input.df,file=conn)
  close(conn)
  input.csv.string <- paste(input.csv.string, collapse='\n')
  output.csv.string <- .jcall(rules.session, 'S', 'execute',input.csv.string)
  conn <- textConnection(output.csv.string, 'r')
  output.df<-read.csv(file=conn, header=T)
  close(conn)
  return(output.df)
}