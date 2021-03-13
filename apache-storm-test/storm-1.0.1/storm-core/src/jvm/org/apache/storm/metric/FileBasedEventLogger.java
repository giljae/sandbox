/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.metric;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileBasedEventLogger implements IEventLogger {
    private static final Logger LOG = LoggerFactory.getLogger(FileBasedEventLogger.class);

    private static final int FLUSH_INTERVAL_MILLIS = 1000;

    private Path eventLogPath;
    private BufferedWriter eventLogWriter;
    private volatile boolean dirty = false;

    private void initLogWriter(Path logFilePath) {
        try {
            LOG.info("Event log path {}", logFilePath);
            eventLogPath = logFilePath;
            eventLogWriter = Files.newBufferedWriter(eventLogPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                                                     StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOG.error("Error setting up FileBasedEventLogger.", e);
            throw new RuntimeException(e);
        }
    }


    private void setUpFlushTask() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    if(dirty) {
                        eventLogWriter.flush();
                        dirty = false;
                    }
                } catch (IOException ex) {
                    LOG.error("Error flushing " + eventLogPath, ex);
                    throw new RuntimeException(ex);
                }
            }
        };

        scheduler.scheduleAtFixedRate(task, FLUSH_INTERVAL_MILLIS, FLUSH_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    private String getFirstNonNull(String... strings) {
        for (String str : strings) {
            if (str != null) {
                return str;
            }
        }
        return null;
    }

    private String getLogDir(Map stormConf) {
        String logDir = getFirstNonNull(System.getProperty("storm.log.dir"),
                                        (String) stormConf.get("storm.log.dir"));
        if (logDir == null) {
            logDir = Paths.get(getFirstNonNull(System.getProperty("storm.home"),
                                               System.getProperty("storm.local.dir"),
                                               (String) stormConf.get("storm.local.dir"),
                                               StringUtils.EMPTY),
                               "logs").toString();
        }
        return logDir;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        String workersArtifactDir; // workers artifact directory
        String stormId = context.getStormId();
        int port = context.getThisWorkerPort();
        if ((workersArtifactDir = (String) stormConf.get(Config.STORM_WORKERS_ARTIFACTS_DIR)) == null) {
            workersArtifactDir = "workers-artifacts";
        }
        /*
         * Include the topology name & worker port in the file name so that
         * multiple event loggers can log independently.
         */
        Path path = Paths.get(workersArtifactDir, stormId, Integer.toString(port), "events.log");
        if (!path.isAbsolute()) {
            path = Paths.get(getLogDir(stormConf), workersArtifactDir,
                             stormId, Integer.toString(port), "events.log");
        }
        File dir = path.toFile().getParentFile();
        if (!dir.exists()) {
            dir.mkdirs();
        }
        initLogWriter(path);
        setUpFlushTask();
    }

    @Override
    public void log(EventInfo event) {
        try {
            //TODO: file rotation
            eventLogWriter.write(event.toString());
            eventLogWriter.newLine();
            dirty = true;
        } catch (IOException ex) {
            LOG.error("Error logging event {}", event, ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        try {
            eventLogWriter.close();
        } catch (IOException ex) {
            LOG.error("Error closing event log.", ex);
        }
    }
}
