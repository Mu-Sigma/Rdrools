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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

public class DroolsServiceTest extends TestCase {
    private static String[][] RULES_OUTPUT = new String[][] { { "address", "subject", "body" },
            { "johnny@school.edu", "Your grade in Math", "You passed with a 85 in Math, Johnny" },
            { "johnny@school.edu", "Your grade in English", "You failed with a 45 in English, Johnny" },
            { "johnny@school.edu", "Your grade in Science", "You passed with a 72 in Science, Johnny" },
            { "johnny@school.edu", "Your grade in History", "You passed with a 91 in History, Johnny" },
            { "johnny@school.edu", "Your grade in Spanish", "You passed with a 77 in Spanish, Johnny" },
            { "james@school.edu", "Your grade in Math", "You passed with a 99 in Math, James" },
            { "james@school.edu", "Your grade in English", "You passed with a 87 in English, James" },
            { "james@school.edu", "Your grade in Science", "You passed with a 73 in Science, James" },
            { "james@school.edu", "Your grade in History", "You passed with a 80 in History, James" },
            { "james@school.edu", "Your grade in Spanish", "You passed with a 90 in Spanish, James" },
            { "joseph@school.edu", "Your grade in Math", "You failed with a 25 in Math, Joseph" },
            { "joseph@school.edu", "Your grade in English", "You failed with a 47 in English, Joseph" },
            { "joseph@school.edu", "Your grade in Science", "You failed with a 33 in Science, Joseph" },
            { "joseph@school.edu", "Your grade in History", "You passed with a 62 in History, Joseph" },
            { "joseph@school.edu", "Your grade in Spanish", "You passed with a 54 in Spanish, Joseph" } };

    private DroolsService service;

    public DroolsServiceTest() {
    }

    @Test
    public void testDataSetRules() {
        runRules("input.csv", "rules.drl", "name, class, grade, email", "address, subject, body",
                        RULES_OUTPUT);
    }


    public void runRules(String csvFile, String rulesFile, String expectedInputColumnsCSV,
                    String outputColumnsCSV, String[][] expectedValues) {
        try {
            String input = readFileAsString(this.getClass().getResourceAsStream(csvFile));
            String rules = readFileAsString(this.getClass().getResourceAsStream(rulesFile));
            service = new DroolsService(rules, expectedInputColumnsCSV, outputColumnsCSV);
            String outputCSV = service.execute(input);
            
            CSVParser parser = new CSVParser(new StringReader(outputCSV), CSVFormat.DEFAULT.withFirstRecordAsHeader());
            Set<String> headers = parser.getHeaderMap().keySet(); 
            String[] columns = headers.toArray(new String[headers.size()]);
            
            checkValues(expectedValues[0], columns);

            String[] values = null;
            int index = 0;
            Iterator<CSVRecord> iterator = parser.iterator();
            
            while (iterator.hasNext()) {
                
            	CSVRecord record = iterator.next();
            	values = getRecordAsArray(record);
            	index += 1;
                
                assertTrue("Too many resulting rows: reading line " + index, index < expectedValues.length);
                checkValues(expectedValues[index], values);
            }
            assertEquals("Too few rows", expectedValues.length - 1, index);
            parser.close();
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals("Error occured!", 1, 2);
        }

    }
    
    
	public String[] getRecordAsArray(CSVRecord record) {
		
		ArrayList<String> fieldsList = new ArrayList<String>();
		for(int i = 0; i < record.size(); i++) {
			fieldsList.add(record.get(i));
		}
		String[] recordAsArray = fieldsList.toArray(new String[fieldsList.size()]);
		System.out.println(fieldsList);
		return(recordAsArray);
	}
	

    public String readFileAsString(InputStream inputStream) throws java.io.IOException {
        InputStreamReader inRead = new InputStreamReader(inputStream);
        StringBuffer fileData = new StringBuffer(1000);
        BufferedReader reader = new BufferedReader(inRead);
        char[] buf = new char[1024];
        int numRead = 0;
        while ((numRead = reader.read(buf)) != -1) {
            fileData.append(buf, 0, numRead);
        }
        reader.close();
        return fileData.toString();
    }

    
    public void checkValues(String[] expected, String[] actual) {
        if (expected == null) {
            assertNull(actual);
        } else {
            assertNotNull(actual);
            assertEquals(expected.length, actual.length);
            for (int index = 0; index < expected.length; index++) {
                assertEquals("Column at index " + index, expected[index], actual[index]);
            }
        }
    }
}
