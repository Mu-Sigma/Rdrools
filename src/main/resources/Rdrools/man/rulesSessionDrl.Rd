\name{rulesSessionDrl}
\alias{rulesSessionDrl}

\title{
Creates a session of the rules engine
}
\description{
The rulesSession creates a session that interfaces between R and the Drools engine. The session is utilized by the runRules function for executing a data frame against a set of rules.
}
\usage{
rulesSessionDrl(rules, inputColumns, outputColumns)
}
\arguments{
 \item{rules}{
     a character vector consisting of lines read from a rules file of format drl(drools rules file)\cr
     This character vector is eventually collapsed into a character vector of length 1, so the way you read the file could potentially be just about anything
}
  \item{inputColumns}{
     a character vector of a set of input column, for example\cr
     \code{inputColumns<-c('name', 'class', 'grade', 'email')} 
}
  \item{outputColumns}{
	a character vector of a set of expected output columns, for example\cr
	\code{outputColumns<-c('address', 'subject', 'body')}
}
}
\details{
An active drools rules session. This promotes re-usability of a session, i.e. you can utilize the same session repetitively for different data sets of the same format.
}
\value{
\item{rules.session.object}{Returns a session to the rules engine}
}
\author{
Ashwin Raaghav <ashraaghav@gmail.com>, SMS Chauhan <smschauhah@gmail.com>
}
\note{
Please have a look at the examples provided in the 'examples' section of the \code{\link{Rdrools}} man page. A sample data set and a set of rules have been supplied help you understand the package usage.
}

\seealso{
\code{\link{runRulesDrl}}, \code{\link{Rdrools}}
}
\examples{
library(Rdrools)
data(rules)
inputColumns<-c('name', 'class', 'grade', 'email')
outputColumns<-c('address', 'subject', 'body')
rulesSession<-rulesSession(rules, inputColumns, outputColumns)
}
\keyword{ rulesSessionDrl }
\keyword{ runRulesDrl }
\keyword{ Rdrools }