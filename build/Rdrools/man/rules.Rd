\name{rules}
\alias{rules}
\docType{data}
\title{
A set of rules, for the \code{\link{class}} dataset
}
\description{
a character vector of lines read using \code{readLines()} from a drools rule file (text files with drools rules)
}
\usage{data(rules)}
\format{
  The expected format is a character vector.
}
\details{
You could create a similar rules character vector by using \cr
\code{rules<-readLines('rules.drl')}
}
\examples{
data(rules)
str(rules)
}
\keyword{datasets}
