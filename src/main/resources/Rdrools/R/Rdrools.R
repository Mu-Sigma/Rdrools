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
#' @param dataset rules defined in a csv file
#' @param rules dataframe 
#' -----------------------------------------------------------------------------
#' @return rules in required format, input columns and output columns
#' 
convertRules <- function(dataset, rules){
  
  sampleData <- sampleData
  
  sampleRules <- sampleRules
  rulesList <- list()
  input.columns <- colnames(sampleData)
  
  output.columns <-  c(input.columns)
  outputCols <- list()
  # Getting the required format to display output columns
  for(k in 1:ncol(sampleData)){
    outputCols[k] <- paste0("output.put('",colnames(sampleData)[k], "',input.get('",colnames(sampleData)[k],"'));")
    
  }
  
  for(i in 1:nrow(sampleRules)){
    
    variables  <- getVariables(sampleRules)
    filterData <- variables[[i]][1]
    
    groupbyColumn  <- variables[[i]][2]
    aggregateCoulmn <- variables[[i]][3]
    
    aggregationFunc <-noquote( variables[[i]][4])
    operation <- variables[[i]][5]
    argument <- variables[[i]][6]
    print(aggregationFunc)
    print(groupbyColumn)
    # checking if there are more than one group by
    if(unlist(gregexpr(pattern =',',variables[[i]][2]))!=-1 && variables[[i]][2]!=""){
      
      n <- length(unlist(gregexpr(pattern =',',variables[[i]][2])))
      groupbyCondition <- list()
      
      
      for(j in 0:n){
        #making groupby condition if there are multiple groupby
        groupbyColumn <- unlist(strsplit(unlist(groupbyColumn),","))
        groupbyCondition[j+1] <- paste0(groupbyColumn[j+1],'==input.get("',groupbyColumn[j+1],'")')
        groupbyCondition <-paste0(groupbyCondition,collapse = ",")
        
        if(filterData==""){
          #condition when there is no filter
          print(paste0("no filter & multple group by",i))
          accumulateCondition <-    paste0("result: Double()
                                           from accumulate($condition:HashMap(",groupbyCondition,"),",
                                           aggregationFunc,"(Double.valueOf($condition.get(",shQuote(aggregateCoulmn),").toString())))") 
          
          
          
          
        }else{
          #condition when there is filter
          print(paste0("filter & multple group by",i))
          accumulateCondition <-  paste0("result: Double()
                                         from accumulate($condition:HashMap(",groupbyCondition,",", 
                                         filterData,"),",
                                         aggregationFunc,"(Double.valueOf($condition.get(",shQuote(aggregateCoulmn),").toString())))")  
        }
      }}else if(unlist(gregexpr(pattern =',',variables[[i]][2]))==-1 && variables[[i]][2]==""){
        
        print("im in elseif")
        print(aggregationFunc)
        
        if(aggregationFunc=="compare" && filterData==""){
          
          print(paste0("no filter & no group by,compare",i))
          accumulateCondition <- paste0('result:HashMap(Double.valueOf(this["',aggregateCoulmn,'"]) ',operation,' Double.valueOf(this["',argument,'"]))')
          
        }else if(aggregationFunc=="compare" && filterData!=""){
          accumulateCondition <- paste0('result:HashMap(',filterData,',Double.valueOf( this["',aggregateCoulmn,'"]) ',operation,' Double.valueOf(this["',argument,'"]))')
        }
      }else{
        
        groupbyColumn <- groupbyColumn
        groupbyCondition <- paste0(groupbyColumn,'==input.get("',groupbyColumn,'")')
        #groupbyOutput <- paste0('output.put('",groupbyColumn, "',input.get('",groupbyColumn,"'));')
        
        
        
        
        if(filterData==""){
          print(paste0("one groupby,no fiklter"),i)
          accumulateCondition <-    paste0("result: Double()
                                           from accumulate($condition:HashMap(",groupbyCondition,"),",
                                           aggregationFunc,"(Double.valueOf($condition.get(",shQuote(aggregateCoulmn),").toString())))")
          
          
          
          
        }else{
          print(paste0("one groupby, fiklter"),i)
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
    for(m in outputCols){#adding the condition for displaying output columns 
      sampleRulesListtest[length(sampleRulesListtest)+1] <- m
    }
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
    
  }
  
  return(list(rulesList,input.columns,output.columns))
}



