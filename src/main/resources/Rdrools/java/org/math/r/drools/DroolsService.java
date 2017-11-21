/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package org.math.r.drools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.drools.core.SessionConfiguration;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import com.Ostermiller.util.CSVParser;
import com.Ostermiller.util.CSVPrinter;

/**
 * This class creates a KIE Session with the DRL file provided from the R code and executes the rules
 * @author SMS Chauhan
 * @modified Ashwin Raaghav
 */
public class DroolsService {

    private String[] inputColumns = null;

    private List<String> expectedInputColumns = null;

    private List<String> outputColumns = null;

    private KieServices kService = KieServices.Factory.get();
    private KieSession session;

    public DroolsService(String rules, String expectedInputColumnsCSV, String outputColumnsCSV)
                    throws Exception {
        this.expectedInputColumns = new ArrayList<String>();
        this.outputColumns = new ArrayList<String>();
        // validate all inputs
        validateInputs(rules, expectedInputColumnsCSV, expectedInputColumns, outputColumnsCSV,
                        outputColumns);
        this.session = createSession(rules);
    }

    public String execute(String inputCSV) throws Exception {
        String outputCSV = null;
        ByteArrayOutputStream csvOutputStream = new ByteArrayOutputStream();

        // verify input CSV
        validateInputCSV(inputCSV);

        // read the inputs columns from the input CSV string
        InputStream inputCSVStream = new ByteArrayInputStream(inputCSV.getBytes());
        CSVParser parser = new CSVParser(inputCSVStream);
        this.inputColumns = parser.getLine();

        // verify input columns
        verifyInputColumns(this.expectedInputColumns, this.inputColumns);

        // open an output stream for the output CSV string and print the column names
        CSVPrinter printer = new CSVPrinter(csvOutputStream);
        printer.writeln(this.outputColumns.toArray(new String[this.outputColumns.size()]));

        // get a drools session from the rules file
        // run the rules and write the results to the output CSV
        runRules(this.inputColumns, this.outputColumns, parser, printer, this.session);
        
        // convert the ByteArrayOutputStream to a string
        outputCSV = new String(csvOutputStream.toByteArray(), "UTF-8");

        return outputCSV;

    }

    protected void validateInputCSV(String inputCSV) throws Exception {
        if (inputCSV == null || inputCSV == "") {
            throw new IllegalArgumentException("The input dataset is emtpy!");
        }
    }

    protected void validateInputs(String rules, String expectedInputColumnsCSV,
                    List<String> expectedInputColumns, String outputColumnsCSV, List<String> outputColumns)
                    throws Exception

    {
        if (rules == null || rules == "") {
            throw new IllegalArgumentException("Empty rules file!");
        }
        StringTokenizer tokenizer = null;
        if (expectedInputColumnsCSV != null && expectedInputColumnsCSV != "") {

            tokenizer = new StringTokenizer(expectedInputColumnsCSV.replace(" ", ""), ",");

            while (tokenizer.hasMoreTokens()) {
                expectedInputColumns.add(tokenizer.nextToken());
            }
        } else {
            throw new IllegalArgumentException("No input columns found!");
        }
        if (expectedInputColumnsCSV != null && expectedInputColumnsCSV != "") {
            tokenizer = new StringTokenizer(outputColumnsCSV.replace(" ", ""), ",");

            while (tokenizer.hasMoreTokens()) {
                outputColumns.add(tokenizer.nextToken());
            }
        } else {
            throw new IllegalArgumentException("No output columns found!");
        }
    }

    protected void verifyInputColumns(List<String> expectedInputColumns, String[] inputColumns)
                    throws Exception {
        for (String column : expectedInputColumns) {
            boolean found = false;
            for (int index = 0; index < inputColumns.length; index++) {
                if (inputColumns[index].equals(column)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException(
                                "Error: The input CSV file does not contain the column '" + column
                                                + "' which is required by the rules file!");
            }
        }
    }

    protected KieSession createSession(String rules) throws IllegalArgumentException {
        
    	try {
            
    		KieFileSystem kfs = kService.newKieFileSystem();
        	kfs.write("/src/main/resources/rule.drl", kService.getResources().newReaderResource( new StringReader(rules) ));
            KieBuilder builder = kService.newKieBuilder(kfs).buildAll();
        	
            if (builder.getResults().hasMessages(Message.Level.ERROR)) {
            	throw new IllegalArgumentException("Error parsing rules file: " + builder.getResults().toString());
            }

    	    KieContainer kContainer = kService.newKieContainer(kService.getRepository().getDefaultReleaseId());
    	    SessionConfiguration kConf = SessionConfiguration.getDefaultInstance();
    	    session = kContainer.newKieSession(kConf);

            return session;
        } finally {

        }
    }

    protected void runRules(String[] inputColumns, List<String> outputColumns, CSVParser parser,
                    CSVPrinter printer, KieSession session) throws Exception {
        String[] outputRow = new String[outputColumns.size()];
        String[] inputRow;

        Map<String, String> outputMap = new HashMap<String, String>();

        session.setGlobal("output", outputMap);

        while ((inputRow = parser.getLine()) != null) {
            // write the values for the input row
        	Map<String, String> inputMap = new HashMap<String, String>();
            for (int index = 0; index < inputColumns.length; index++) {
                inputMap.put(inputColumns[index], inputRow[index]);
            }
            if (outputMap.size() > 0) {
                outputMap.clear();
            }
            session.insert(inputMap);
            session.fireAllRules();
            if (outputMap.size() > 0) {
                for (int index = 0; index < outputColumns.size(); index++) {
                    outputRow[index] = outputMap.get(outputColumns.get(index));
                }
                printer.writeln(outputRow);
            }
        }
    }
}
