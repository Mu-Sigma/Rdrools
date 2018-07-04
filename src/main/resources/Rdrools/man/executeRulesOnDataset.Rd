\name{executeRulesOnDataset}
\alias{executeRulesOnDataset}

\title{
Runs the data science specific rules
}
\description{
This function is specifically for running data science specific rules. Rules are applied on an input data frame and the results are returned as the output of the function. The rules are to be defined in a csv file. The rules engine runs the rules on the inputted dataset and stores the results in a list.
}
\usage{
executeRulesOnDataset(dataset, rules)
}

\arguments{
  \item{dataset}{
  a dataframe consisting of a set of rows you wish to transform, and columns you wish to use in the transformation
}
  \item{rules}{
    rules given in a csv file
}
}
\details{
For further details about the rules file, look at the vignette
}
\value{
  a list of input, intermediate output and output per rule
 input: a tibble containing the rule defined by the user
 intermediate output: is an empty tibble when there is no groupby and a tibble 
                      with the aggregated value for each groupw when there
                      is a groupby
 output: a tibble with 3 columns:
 
   Group: represents group name when there is a groupby and the row number when 
        there is no groupby
   Indices: the row numbers in each group when there is a groupby and row 
           numbers of the dataframe when there is no groupby
   IsTrue: flag to say if the data point is satisfying the rule or not.
          Gives true if the point or group satisfies the rule and false if not.  

}

\author{
Dheekshitha PS <dheekshitha119@gmail.com>
}

\seealso{
\code{\link{runRulesDrl}}, \code{\link{Rdrools}}
}
\examples{
library(Rdrools)
data(transactionsData)
data(transactionsRules)
executeRulesOnDataset(transactionsData, transactionsRules)

}
\keyword{ rulesSessionDrl }
\keyword{ runRulesDrl }
\keyword{ Rdrools }
