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
