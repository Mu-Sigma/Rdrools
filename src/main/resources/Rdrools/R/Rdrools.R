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
  #adding row numbers to the dataframe
  dataset$rowNumber <- 1:nrow(dataset)
  rules <-changecolnamesInRules(dataset = dataset, rules = rules )
  #converting factors to character
  rules[] <- lapply(rules, as.character)
  #getting input and output columns
  input.columns <- getrequiredColumns(dataset,rules)[[1]]
  output.columns <- getrequiredColumns(dataset,rules)[[2]]
  #changing the column names of the dataset by removing . or _ present in the column names
  colnames(dataset) <- input.columns
  rulesList <- list()
  outputCols <- list()
  # Getting the required format to display output columns
  outputCols <-  map(colnames(dataset),function(x)paste0("output.put('",x, "',input.get('",x,"'));"))
  csvFormatOfEachRule <- list()
  outputDf <- list()
  outputWithAllRows <- list()
  outputDfForEachRule <- list()
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
    ruleValue <- paste0("Rule",i,"Value")
    if(aggregationFunc=="compare"){
      drlRules[length(drlRules)+1] <- paste0("output.put(\"Rule",i,"\",'",aggregateCoulmn,operation,argument,"');")  
      drlRules[length(drlRules)+1] <- paste0("output.put(",shQuote(ruleValue),",result);")  
    }else{
      drlRules[length(drlRules)+1] <- paste0("output.put(\"Rule",i,"\",result",operation,argument,");")      
      drlRules[length(drlRules)+1] <- paste0("output.put(",shQuote(ruleValue),",result);")  
    }
    drlRules[length(drlRules)+1] <-'end'
    rulesList[[i]] <- drlRules
    csvFormatOfEachRule[[i]] <- rules[i,]
    
    if(filterData != ""){
      
      filteredData <- getDrlForFilterRules(dataset,rules,i,outputCols,input.columns,output.columns)
      ruleName <- paste0("Rule",i)
      
      filteredData <- filteredData[,c(input.columns,ruleName,ruleValue)]
      filteredDataTrue <-filter_(filteredData,paste(ruleName,"==","'true'"))
      filteredDataFalse <-filter_(filteredData,paste(ruleName,"==","'false'"))
      
      if(aggregationFunc==""){#only filter
        outputDf[[i]] <- filteredData
        filteredDataFalse <- NULL
      }else{
        inputData <- filteredDataTrue
        rules.Session <- rulesSession(unlist(drlRules),input.columns,output.columns)
        outputDf[[i]] <- runRules(rules.Session,inputData)  
        
      }
      
    }else{
      print(drlRules)
      inputData <- dataset
      filteredDataFalse <- NULL
      rules.Session <- rulesSession(unlist(drlRules),input.columns,output.columns)
      outputDf[[i]] <- runRules(rules.Session,inputData)  
      print(outputDf)
      
    }
    
    outputWithAllRows[[i]] <- formatOutput(dataset = dataset,outputDf = outputDf[[i]],rules = rules,filteredDataFalse =filteredDataFalse ,input.columns=input.columns,ruleNum=i)$outputDf
    outputDfForEachRule[[i]] <- formatOutput(dataset = dataset,outputDf = outputDf[[i]],rules = rules,filteredDataFalse =filteredDataFalse ,input.columns=input.columns,ruleNum=i)[[2]]
    
  }

  intermediateOutput <- list(rep(NULL, nrow(rules)))
  
  output <- mapply(list,
                   input=csvFormatOfEachRule,
                   intermediateOutput = intermediateOutput,
                   output=outputDfForEachRule, SIMPLIFY = F)
  return(output)
  
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
  #removing the . or _ in the column names of the dataset
  colnames(dataset) <- gsub('\\.|\\_', '', colnames(dataset))
  input.columns <- colnames(dataset)
  output.columns <-list()
  valueColumns <- list()
  j<- ncol(dataset)
  #adding one column for each rule
  output.columns<-lapply(1:nrow(rules), function(i){
    output.columns[j+i] <- paste0("Rule",i)
    
  })
  #adding columns to store the result value for each rule
  valueColumns<-lapply(1:nrow(rules), function(n){
    valueColumns[n] <- paste0("Rule",n,"Value")
    
  })
  output.columns <- unlist(append(input.columns,output.columns))
  output.columns <- unlist(append(output.columns,valueColumns))
  
  
  return(list(input.columns=input.columns,output.columns=output.columns))
}

#' -----------------------------------------------------------------------------
#' @description: This function is used to remove the . or _ in column names of the dataset present 
#' in the rules file
#' -----------------------------------------------------------------------------
#' @param dataset rules defined in a csv file
#' @param rules dataframe 
#' -----------------------------------------------------------------------------
#' @return rules in required format
#' 

changecolnamesInRules <-function(dataset,rules){
  
  formattedRules <- rules
  for (col.name in colnames(dataset)) {
    formattedRules<-data.frame(
      lapply(formattedRules, function(x){
        gsub(pattern = col.name, #checking for the column names of dataset present in rules
             replacement = gsub(pattern = "\\.|\\_",replacement = "",col.name),
             x)}))#replacing the . or _ 
  }
  return(formattedRules)
  
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
  #filter given
  condition <- paste0("input:HashMap(",filterData,")")
  ruleValue <- paste0("Rule",ruleNum,"Value")
  #defining the complementary condition for the filter given
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
    paste0("output.put('",ruleValue,"',",shQuote(filterData)," );"),
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
  drlRules[length(drlRules)+1] <- paste0("output.put('",ruleValue,"',",shQuote(paste("Not",filterData)),");")
  drlRules[length(drlRules)+1] <-'end'
  
  #calling the rulessession
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
  #adding row number as a column to identify the last and first row for each group
  groupbyColumn  <- rules[ruleNum,"GroupBy"]
  aggregationFunc <-noquote( rules[ruleNum,"Function"])
  ruleName <- paste0("Rule",ruleNum)
  ruleValue <- paste0("Rule",ruleNum,"Value")
  
  if(groupbyColumn!=""){
    
    #getting the first and last row for each group
    outputFormatted <-  eval(parse(text=paste('outputDf%>%group_by(',groupbyColumn,')%>%slice(c(1,n()))%>%ungroup()')))  
    #getting the last row for each group
    outputDfForEachRule <- eval(parse(text=paste('outputDf%>%group_by(',groupbyColumn,')%>%slice(c(n()))%>%ungroup()')))
    #adding new column, Indices
    outputDfForEachRule$Indices <- 0
    for(j in 1:nrow(outputDfForEachRule)){
      lowerRange<-outputFormatted[2*j-1,"rowNumber"]
      upperRange <- outputFormatted[2*j,"rowNumber"]
      #setting all the rows of group as true/false according to the last row of each group
      ifelse(outputFormatted[,ruleName][2*j,]=='true',outputDf[c(as.numeric(lowerRange):as.numeric(upperRange)), ruleName] <-"true",outputDf[c(as.numeric(lowerRange):as.numeric(upperRange)), ruleName]  <- "false")
      #getting the required columns from the output
      outputDf <- outputDf[,c(input.columns,ruleName,ruleValue)]
      #adding the filtered out rows with flag False to the final output
      outputDf <- rbind(outputDf,filteredDataFalse)
      groupbyColumn <-unlist(strsplit(groupbyColumn,","))
      outputDfForEachRule <- outputDfForEachRule[,c(groupbyColumn,ruleName,"Indices")]
      #getting the Indices of each group
      outputDfForEachRule[j,"Indices"]<-paste(seq(as.numeric(lowerRange),as.numeric(upperRange)),collapse = ",")
      outputDfForEachRule <- outputDfForEachRule[,c(groupbyColumn,"Indices",ruleName)]
      
    }  
  }else{
    
    if(aggregationFunc != "compare" && aggregationFunc != ""){
      #agg on whole column  
      #getting the last row
      outputFormatted <- outputDf%>%slice(n())
      
      ifelse(outputFormatted[,ruleName][1,]=='true',outputDf[c(1:nrow(outputDf)), ruleName] <-"true",outputDf[c(1:nrow(outputDf)), ruleName]  <- "false")
      #getting the required columns
      outputDf <- outputDf[,c(input.columns,ruleName,ruleValue)]
      outputDf <- rbind(outputDf,filteredDataFalse)
      outputDfForEachRule <- outputFormatted
      outputDfForEachRule$Group <- 1
      outputDfForEachRule$Indices <- paste(outputDf[,"rowNumber"],collapse = ",")
      outputDfForEachRule <- outputDfForEachRule[,c("Group","Indices",ruleName)]
      
    }else{
      
      outputDf <- outputDf[,c(input.columns,ruleName,ruleValue)]
      outputDf <- rbind(outputDf,filteredDataFalse)
      outputDfForEachRule <- outputDf
      outputDfForEachRule$Group <-outputDf$rowNumber
      outputDfForEachRule$Indices <- outputDf$rowNumber
      outputDfForEachRule <- outputDfForEachRule[,c("Group","Indices",ruleName)]
      
    }
    
  }
  outputDfForEachRule <- setNames(outputDfForEachRule, c("Group","Indices","IsTrue"))
  
  return(list(outputDf=outputDf,outputDfForEachRule=outputDfForEachRule))
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