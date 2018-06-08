

#' -----------------------------------------------------------------------------
#' @description: This function is used to extract the required parameters from the rules data
#'               
#' -----------------------------------------------------------------------------
#' @param sampleRules rules defined in a csv file

#' -----------------------------------------------------------------------------
#' @return required parameters like filter, group by etc
#' 
getVariables <-function(sampleRules){
  
  
  sampleRules <- sampleRules
  variablesList <- list()
  variablesforEachRule <- list()
  for(i in 1:nrow(sampleRules)){
    
    #getting the varibles for each rule
    filterData <- sampleRules[i,1]
    groupbyColumn <- sampleRules[i,2]
    aggregateCoulmn <- sampleRules[i,3]
    aggregationFunc <- sampleRules[i,4]
    operation <- sampleRules[i,5]
    argument <- sampleRules[i,6]
    
    
    variablesforEachRule <- list(filterData,groupbyColumn,aggregateCoulmn,aggregationFunc,operation,argument)
    variablesList[i] <- list(variablesforEachRule)
  }  
  
  
  return(variablesList) 
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


