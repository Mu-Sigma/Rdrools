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
#' @return rules in required format, input columns and output columns
#' 
executeRulesOnDataset <- function(dataset,rules){
  
  rulesList <- list()
  outputCols <- list()
  # Getting the required format to display output columns
  outputCols <-  map(colnames(dataset),function(x)paste0("output.put('",x, "',input.get('",x,"'));"))
  
  # Running the loop to get drl format for all the rules
  
  lapply(1:nrow(rules), function(i){
    
    
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
      map(groupbyColumn,function(groupbyColumn){
        #making groupby condition if there are multiple groupby
        groupbyColumn <- unlist(strsplit(unlist(groupbyColumn),","))
        groupbyCondition <- paste0(groupbyColumn,'==input.get("',groupbyColumn,'")')
        groupbyCondition <-paste0(groupbyCondition,collapse = ",")
        
        if(filterData==""){
          #condition when there is no filter
          
          accumulateCondition <-    paste0("result: Double()
                                           from accumulate($condition:HashMap(",groupbyCondition,"),",
                                           aggregationFunc,"(Double.valueOf($condition.get(",shQuote(aggregateCoulmn),").toString())))") 
          
        }else{
          #condition when there is filter
          accumulateCondition <-  paste0("result: Double()
                                         from accumulate($condition:HashMap(",groupbyCondition,",", 
                                         filterData,"),",
                                         aggregationFunc,"(Double.valueOf($condition.get(",shQuote(aggregateCoulmn),").toString())))")  
        }
      })
    }else if(unlist(gregexpr(pattern =',',groupbyColumn))==-1 &&groupbyColumn==""){
      #No groupby  
      if(aggregationFunc=="compare" && filterData==""){
        #Rules having compare and no filter
        
        accumulateCondition <- paste0('result:HashMap(Double.valueOf(this["',aggregateCoulmn,'"]) ',operation,' Double.valueOf(this["',argument,'"]))')
        
      }else if(aggregationFunc=="compare" && filterData!=""){
        #Rules having compare and filter
        accumulateCondition <- paste0('result:HashMap(',filterData,',Double.valueOf( this["',aggregateCoulmn,'"]) ',operation,' Double.valueOf(this["',argument,'"]))')
      }
      
    }else if(aggregationFunc!="compare" && filterData==""){
      #Rules having no compare and no filter i.e. aggregation on a columns
      accumulateCondition <-  paste0("result: Double()
                                     from accumulate($condition:HashMap(),",
                                     aggregationFunc,"(Double.valueOf($condition.get(",shQuote(aggregateCoulmn),").toString())))")
    }else{
      
      groupbyColumn <- groupbyColumn
      groupbyCondition <- paste0(groupbyColumn,'==input.get("',groupbyColumn,'")')
      if(filterData==""){
        accumulateCondition <-    paste0("result: Double()
                                         from accumulate($condition:HashMap(",groupbyCondition,"),",
                                         aggregationFunc,"(Double.valueOf($condition.get(",shQuote(aggregateCoulmn),").toString())))")
        
        
      }else{
        accumulateCondition <- paste0("result: Double()
                                      from accumulate($condition:HashMap(",groupbyCondition,",", 
                                      filterData,"),",
                                      aggregationFunc,"(Double.valueOf($condition.get(",shQuote(aggregateCoulmn),").toString())))")  
      }
      
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
    ruleList <- unlist(rulesList,recursive = FALSE)
    
    
  }
  ) 
  
  
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
  return(list(input.columns,output.columns))
}


#' -----------------------------------------------------------------------------
#' @description: This function is used to call the drools session for rules that are in csv format
#'               
#' -----------------------------------------------------------------------------
#' @param dataset dataframe
#' @param rules rules defined in a csv file
#' -----------------------------------------------------------------------------
#' @return drools session
#' 

rulesSessionCsv<-function(dataset,rules) {
  
  
  #Call getrequiredColumns and executeRulesOnDataset function to get rules, input and output columns
  droolsSession<-.jnew('org/math/r/drools/DroolsService',rules,input.columns, output.columns)
  return(droolsSession)
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