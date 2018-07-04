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
    
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function (x) 
{
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }% use one of  RShowDoc("KEYWORDS")
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
