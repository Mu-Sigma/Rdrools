Introduction
============

Objectives of *Rdrools*
-----------------------

The Rdrools package aims to accomplish two main objectives:

-   Allow data scientists an intuitive interface to **execute business rules on datasets for the purpose of analysis or designing intelligent systems**, while leveraging the Drools rule engine
-   Provide a direct interface to *Drools* for executing all types of rules defined in the *Drools* *.drl* format

The advantages of a rule engine
-------------------------------

Rule engines allow for optimal checking of rules against data for large rule sets \[of the order of hundreds or even thousands of rules\]. *Drools* \[and other rule engines\] implement an enhanced version of the **Rete algorithm**, which efficiently match **facts** \[data tuples\] against **conditions** \[rules\]. This allows for codifying intuition/ business context which can be used to power intelligent systems.

Why Rdrools
-----------

RDrools brings the efficiencies of large-scale production rule systems to data science users. Rule sets can be used alone, or in conjunction with machine learning models, to develop and operationalize intelligent systems. RDrools allows for deployment of rules defined through an R interface into a production system. As data comes in \[periodic or real-time\], a pre-defined set of rules can be checked on the data, and actions can be triggered based on the result

Installation
------------

``` r
#install.packages("Rdrools")
```

Usage
-----

``` r
library(Rdrools)
```

    ## Loading required package: rJava

    ## Loading required package: Rdroolsjars

``` r
data("iris")
data("irisRules")
#Rules
sampleRules <- irisRules
rownames(sampleRules) <- seq(1:nrow(sampleRules))
sampleRules[is.na(sampleRules)]    <-""
sampleRules
```

    ##                   Filters GroupBy       Column Function Operation
    ## 1     Species == 'setosa'                                        
    ## 2                         Species Sepal.Length  average        >=
    ## 3                                 Sepal.Length  average         <
    ## 4         Sepal.Width > 3         Sepal.Length  average        >=
    ## 5       Petal.Width > 0.4 Species Petal.Length  average         <
    ## 6                                 Petal.Length  compare        >=
    ## 7 Species == 'versicolor'         Petal.Length  compare        >=
    ##      Argument
    ## 1            
    ## 2         5.9
    ## 3           5
    ## 4           5
    ## 5           5
    ## 6 Sepal.Width
    ## 7 Sepal.Width

``` r
columnAggregationRule <- sampleRules[3,]
#Executing rules
columnAggregationRuleOutput <- executeRulesOnDataset(iris, columnAggregationRule)
```

    ## List of 1
    ##  $ :List of 20
    ##   ..$ : chr "import java.util.HashMap"
    ##   ..$ : chr "import java.lang.Double"
    ##   ..$ : chr "global java.util.HashMap output"
    ##   ..$ : chr ""
    ##   ..$ : chr "  dialect \"mvel\""
    ##   ..$ : chr "rule \"Rule1\""
    ##   ..$ : chr "       salience 0"
    ##   ..$ : chr "       when"
    ##   ..$ : chr "        input: HashMap()"
    ##   ..$ : chr "result: Double()\n                               from accumulate($condition:HashMap(),average(Double.valueOf($c"| __truncated__
    ##   ..$ : chr "then"
    ##   ..$ : chr "output.put('SepalLength',input.get('SepalLength'));"
    ##   ..$ : chr "output.put('SepalWidth',input.get('SepalWidth'));"
    ##   ..$ : chr "output.put('PetalLength',input.get('PetalLength'));"
    ##   ..$ : chr "output.put('PetalWidth',input.get('PetalWidth'));"
    ##   ..$ : chr "output.put('Species',input.get('Species'));"
    ##   ..$ : chr "output.put('rowNumber',input.get('rowNumber'));"
    ##   ..$ : chr "output.put(\"Rule1\",result<5);"
    ##   ..$ : chr "output.put('Rule1Value',result);"
    ##   ..$ : chr "end"

``` r
#Output
columnAggregationRuleOutput[[1]]$output
```

    ## # A tibble: 1 x 3
    ##   Group Indices                                                     IsTrue
    ##   <dbl> <chr>                                                       <fct> 
    ## 1     1 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,2… false

Build project to a *.jar
------------------------
Rdrools project is a Java maven project that includes a Java service that interacts with the Rdrools package and the Rdrools jar to provide an intuitive interface to execute business rules on datasets for the purpose of analysis or designing intelligent systems, while leveraging the Drools rule engine

The Java service, Rdrools package and Rdrools jar can be found at **src/main/java/org/math/r/drools/DroolsService.java**, **Rdrools/src/main/resources/Rdroolsjars** and **Rdrools/src/main/resources/Rdrools** locations respectively.

Any changes made to the Java project will reflect only after building the jar, to build jar follow the steps:

-   Install maven in your workstation
-   Git clone this project to your local workstation
-   Go to root directory of the project and run **'mvn clean'**, this would clean up the jar
-   Run **'mvn install'**, this would download all dependent third party jars mentioned in the pom.xml, post installation it will run **'mvn build'** and automatically build the new jar and place it in the **target** folder in root directory
 
Detailed Example - R vignette
-------------------------------
For more detailed example for the R package, please refer to the vignette available at the project path **src/main/resources/Rdrools/vignettes/Executing_Business_Rules_At_Scale_With_RDrools_using_Drools.Rmd**

Report a bug
------------------------
For any bugs or feedback, kindly raise the issue in GitHub page - **https://github.com/Mu-Sigma/Rdrools/issues**
