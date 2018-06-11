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

rulesSession<-function(rules,input.columns, output.columns) {
  rules <- paste(rules, collapse='\n')
  input.columns <- paste(input.columns,collapse=',')
  output.columns <- paste(output.columns,collapse=',')
  droolsSession<-.jnew('org/math/r/drools/DroolsService',rules,input.columns, output.columns)
  return(droolsSession)
}

runRules<-function(rules.session,input.df) {
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



#' -----------------------------------------------------------------------------
#' @description: This function is used to convert the rules data uploaded into required format
#'               
#' -----------------------------------------------------------------------------
#' @param sampleData rules defined in a csv file
#' @param sampleRules dataframe 
#' -----------------------------------------------------------------------------
#' @return rules in required format, input columns and output columns
#' 
convertRules <- function(sampleData,sampleRules){
  
  sampleData <- sampleData
  sampleRules <- sampleRules
  rulesList <- list()
  input.columns <- colnames(sampleData)
  output.columns <-  c(input.columns)
  outputCols <- list()
  # Getting the required format to display output columns
  outputCols <-  map(colnames(dataset),function(x)paste0("output.put('",x, "',input.get('",x,"'));"))
  
  # Running the loop to get drl format for all the rules
  
  map(1:nrow(rules), function(i){
    
    
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
    
    sampleRulesListtest <-list(
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
    sampleRulesListtest <-  append(sampleRulesListtest, outputCols)
    
    if(aggregationFunc=="compare"){
      sampleRulesListtest[length(sampleRulesListtest)+1] <- paste0("output.put(\"Rule",i,"\",'",aggregateCoulmn,operation,argument,"');")  
    }else{
      sampleRulesListtest[length(sampleRulesListtest)+1] <- paste0("output.put(\"Rule",i,"\",result",operation,argument,");")      
    }
    
    sampleRulesListtest[length(sampleRulesListtest)+1] <-'end'
    j<- ncol(sampleData)
    #adding each column for each rule
    output.columns[j+i] <- paste0("Rule",i)
    rulesList[[i]] <- sampleRulesListtest
    ruleList <- unlist(rulesList,recursive = FALSE)
    
    return(list(rulesList,input.columns,output.columns))
    }
      ) 
  
  
}