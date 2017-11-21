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

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.drools.core.SessionConfiguration;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

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

    /*
     * Main function that to convert the input string CSV into a fact, execute the rules and return the output as a CSV string 
     */
    public String execute(String inputCSV) throws Exception {
        String outputCSV = null;

        // verify input CSV
        validateInputCSV(inputCSV);

        // read the inputs columns from the input CSV string
        CSVParser parser = new CSVParser(new StringReader(inputCSV), CSVFormat.DEFAULT.withHeader());
        Set<String> header = parser.getHeaderMap().keySet(); 
        this.inputColumns = header.toArray(new String[header.size()]); 

        // verify input columns
        verifyInputColumns(this.expectedInputColumns, this.inputColumns);

        // open an output stream for the output CSV string and print the column names
        ByteArrayOutputStream csvOutputStream = new ByteArrayOutputStream();
        Writer out = new BufferedWriter(new OutputStreamWriter(csvOutputStream));
        CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withRecordSeparator('\n'));
        
        // Add the header to the output stream
        printer.printRecord(this.outputColumns);

        // get a drools session from the rules file
        // run the rules and write the results to the output CSV
        runRules(this.inputColumns, this.outputColumns, parser, printer, this.session);
        
        // convert the ByteArrayOutputStream to a string
        
        printer.flush();
        outputCSV = new String(csvOutputStream.toByteArray(), "UTF-8");
        
        parser.close();
        printer.close();
        
        return outputCSV;

    }

    /*
     * Checks if the input CSV string is empty
     */
    protected void validateInputCSV(String inputCSV) throws Exception {
        if (inputCSV == null || inputCSV == "") {
            throw new IllegalArgumentException("The input dataset is emtpy!");
        }
    }

    /*
     * Checks if the given rules string and input and output columns are empty
     */
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

    /*
     * Checks if the input columns in the data and the input columns given as parameter match
     */
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

    /*
     * Create a new KIE session for Drools and load the rules into it
     */
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

    /*
     * Add the facts into session and fire all the rules that have been loaded
     * Print the output into the output CSV
     */
    protected void runRules(String[] inputColumns, List<String> outputColumns, CSVParser parser,
                    CSVPrinter printer, KieSession session) throws Exception {
        
    	List<String> outputRow = new ArrayList<String>(outputColumns.size());
        CSVRecord inputRow;
        Iterator<CSVRecord> iterator = parser.iterator();

        Map<String, String> outputMap = new HashMap<String, String>();

        session.setGlobal("output", outputMap);

        while (iterator.hasNext()) {
        	
        	// write the values for the input map
        	inputRow = iterator.next();
        	Map<String, String> inputMap = new HashMap<String, String>();
            
        	for (int index = 0; index < inputColumns.length; index++) {
                inputMap.put(inputColumns[index], inputRow.get(index));
            }
            
        	// Clear output map
        	if (outputMap.size() > 0) {
                outputMap.clear();
            }
            
            // Insert fact into KIE session
            session.insert(inputMap);
            
            // Run all the rules for the fact
            session.fireAllRules();
            
            // Add output to the output stream
            if (outputMap.size() > 0) {
                for (int index = 0; index < outputColumns.size(); index++) {
                    outputRow.add(index, outputMap.get(outputColumns.get(index)));
                }
                printer.printRecord(outputRow);
                outputRow.clear();
            }
        }
    }
}
