#' Sample rules for iris dataset
#'
#' A dataset containing the sample rules to be applied on the iris dataset, where each
#' row represents a rule 
#'
#' @format A data frame with 7 rows and 6 variables:
#' \describe{
#'   \item{Filters}{Filters to be applied in a specific rule}
#'   \item{GroupBy}{Grouping conditions to be appled in a specific rule}
#'   \item{Column}{The variable on which the function is to applied in a specific rule}
#'   \item{Function}{Function to be applied on the specific variable in a rule}
#'   \item{Operation}{The comparison operation to be performed to check the rule}
#'   \item{Argument}{The value agains which the comparison operation is to be performed}
#'   }
"irisRules"

#' Sample rules for the sample transaction dataset (transactionData)
#'
#' A dataset containing the sample rules to be applied on the transaction dataset, where each
#' row represents a rule 
#'
#' @format A data frame with 7 rows and 6 variables:
#' \describe{
#'   \item{Filters}{Filters to be applied in a specific rule}
#'   \item{GroupBy}{Grouping conditions to be appled in a specific rule}
#'   \item{Column}{The variable on which the function is to applied in a specific rule}
#'   \item{Function}{Function to be applied on the specific variable in a rule}
#'   \item{Operation}{The comparison operation to be performed to check the rule}
#'   \item{Argument}{The value agains which the comparison operation is to be performed}
#'   \item{caseSensitive}{Whether column name is case sensitive}
#'   }
"transactionRules"

#' Sample transaction data
#'
#' A dataset containing banking transactions, with variables such as number of transactions, transaction value,
#' and so on 
#'
#' @format A data frame with 39999 rows and 16 variables:
"transactionData"

#' Sample rules in DRL format
#'
#' A list of character strings, providing an example for rules in the DRL format accepted by the 
#' Drools engine
"rules"

#' Sample data of students' grades 
#' 
#' A dataset containing students' grades in different subjects
#' @format A data frame with 15 observations and 5 variables
#' \describe{
#'   \item{id}{ID of the student}
#'   \item{name}{Name of the student}
#'   \item{class}{Subject for which the grade is specified}
#'   \item{grade}{Grade in the subject}
#'   \item{email}{E-mail ID of the student}
#'   }
"class"