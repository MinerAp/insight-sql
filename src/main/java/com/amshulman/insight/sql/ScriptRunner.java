package com.amshulman.insight.sql;

/*
 * Slightly modified version of the com.ibatis.common.jdbc.ScriptRunner class
 * from the iBATIS Apache project. Only removed dependency on Resource class
 * and a constructor 
 */
/*
 *  Copyright 2004 Clinton Begin
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Tool to run database scripts
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ScriptRunner {

    private static final String DEFAULT_DELIMITER = ";";

    /**
     * Runs an SQL script (read in using the Reader parameter)
     * 
     * @param reader - the source of the script
     */
    public static void runScript(Connection connection, Reader reader, boolean stopOnError, boolean autoCommit) throws IOException, SQLException {
        try {
            boolean originalAutoCommit = connection.getAutoCommit();
            try {
                if (originalAutoCommit != autoCommit) {
                    connection.setAutoCommit(autoCommit);
                }
                processScript(connection, reader, stopOnError, autoCommit);
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (IOException | SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error running script.  Cause: " + e, e);
        }
    }

    /**
     * Runs an SQL script (read in using the Reader parameter) using the
     * connection passed in
     * 
     * @param conn - the connection to use for the script
     * @param reader - the source of the script
     * @throws SQLException if any SQL errors occur
     * @throws IOException if there is an error reading from the Reader
     */
    private static void processScript(Connection conn, Reader reader, boolean stopOnError, boolean autoCommit) throws IOException, SQLException {
        String currentDelimiter = ";";

        StringBuffer command = null;
        try {
            LineNumberReader lineReader = new LineNumberReader(reader);
            String line = null;
            while ((line = lineReader.readLine()) != null) {
                if (command == null) {
                    command = new StringBuffer();
                }

                String trimmedLine = line.trim();

                if (trimmedLine.startsWith("--") || trimmedLine.startsWith("//") || trimmedLine.isEmpty()) {
                    // Do nothing
                } else if (trimmedLine.toUpperCase().startsWith("DELIMITER")) {
                    currentDelimiter = trimmedLine.substring(trimmedLine.indexOf(' ') + 1);
                } else {
                    command.append(line.replace(currentDelimiter, DEFAULT_DELIMITER));

                    if (trimmedLine.endsWith(DEFAULT_DELIMITER) || trimmedLine.endsWith(currentDelimiter)) {
                        runCommand(conn, command.substring(0, command.lastIndexOf(DEFAULT_DELIMITER)), stopOnError, autoCommit);
                        command = null;
                    } else {
                        command.append('\n');
                    }
                }
            }
            if (!autoCommit) {
                conn.commit();
            }
        } catch (SQLException | IOException e) {
            e.fillInStackTrace();
            System.err.println("Error executing: " + command);
            System.err.println(e);
            throw e;
        } finally {
            conn.rollback();
        }
    }

    private static void runCommand(Connection conn, String command, boolean stopOnError, boolean autoCommit) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            if (stopOnError) {
                statement.execute(command);
            } else {
                try {
                    statement.execute(command);
                } catch (SQLException e) {
                    e.fillInStackTrace();
                    System.err.println("Error executing: " + command);
                    System.err.println(e);
                }
            }

            if (autoCommit && !conn.getAutoCommit()) {
                conn.commit();
            }

            Thread.yield();
        }
    }
}
