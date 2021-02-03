/*
 * Copyright (c) 2011-2021 MetaSolutions AB <info@metasolutions.se>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.entrystore.rowstore;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PatternOptionBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.Server;
import org.restlet.data.Protocol;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

public abstract class RowStoreApplicationStandalone extends Application {

    private final static org.slf4j.Logger log = LoggerFactory.getLogger(RowStoreApplicationStandalone.class);

    public static String ENV_CONNECTOR_PARAMS = "ROWSTORE_CONNECTOR_PARAMS";

    public static void main(String[] args) {
        System.setProperty("org.restlet.engine.loggerFacadeClass", "org.restlet.ext.slf4j.Slf4jLoggerFacade");

        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption(Option.builder("c").
                longOpt("config").
                required(System.getenv(RowStoreApplication.ENV_CONFIG_URI) == null).
                desc("URL of configuration file, may be omitted if environment variable ROWSTORE_CONFIG_URI is set").
                hasArg().
                argName("URL").
                optionalArg(false).
                type(PatternOptionBuilder.URL_VALUE).
                build());
        options.addOption(Option.builder("p").
                longOpt("port").
                desc("port to listen on; default: 8282").
                hasArg().
                argName("PORT").
                optionalArg(false).
                type(PatternOptionBuilder.NUMBER_VALUE).
                build());
        options.addOption(Option.builder("l").
                longOpt("log-level").
                desc("log level, one of: ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF; default: INFO").
                hasArg().
                argName("LEVEL").
                optionalArg(false).
                build());
        options.addOption(Option.builder().
                longOpt("connector-params").
                desc("comma separated list of parameters to be used for the server connector, " +
                        "the environment variable ROWSTORE_CONNECTOR_PARAMS may be used instead. " +
                        "Example for Jetty: \"threadPool.minThreads=50,threadPool.maxThreads=250\"; " +
                        "see the JavaDoc of JettyServerHelper for available parameters").
                hasArg().
                argName("SETTINGS").
                optionalArg(false).
                build());
        options.addOption(Option.builder("h").longOpt("help").desc("display this help").build());

        CommandLine cl = null;
        try {
            cl = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage() + "\n");
            printHelp(options);
            System.exit(1);
        }

        if (cl.hasOption("help")) {
            printHelp(options);
            System.exit(0);
        }

        String strPort = cl.getOptionValue("p", "8282");
        int port = 8282;
        try {
            port = Integer.parseInt(strPort);
        } catch (NumberFormatException nfe) {
            System.err.println("Invalid port number, must be integer: " + strPort + "\n");
            printHelp(options);
            System.exit(1);
        }

        URI config = null;
        try {
            if (cl.hasOption("c")) {
                config = new URI(cl.getOptionValue("c"));
            } else {
                config = new URI(System.getenv(RowStoreApplication.ENV_CONFIG_URI));
            }
        } catch (URISyntaxException e) {
            System.err.println("Invalid configuration URL: " + e.getMessage() + "\n");
            printHelp(options);
            System.exit(1);
        }

        configureLogging(cl.getOptionValue("log-level", "INFO"));

        Component component = new Component();
        Server server = component.getServers().add(Protocol.HTTP, port);

        String conParams;
        if (cl.hasOption("connector-params")) {
            conParams = cl.getOptionValue("connector-params");
        } else {
            conParams = System.getenv(ENV_CONNECTOR_PARAMS);
        }
        if (conParams != null) {
            for (String param : conParams.split(",")) {
                if (param.length() > 0) {
                    String[] kv = param.split("=");
                    if (kv.length == 2) {
                        log.debug("Adding connector parameter: {}={}", kv[0], kv[1]);
                        server.getContext().getParameters().add(kv[0].trim(), kv[1].trim());
                    } else {
                        System.err.println("Invalid connector parameter: " + param);
                        System.exit(1);
                    }
                }
            }
        }

        component.getLogService().setResponseLogFormat("{ciua} \"{m} {rp} {rq}\" {S} {ES} {es} {hh} {cig} {fi}");
        server.getContext().getParameters().add("useForwardedForHeader", "true");
        component.getClients().add(Protocol.HTTP);
        component.getClients().add(Protocol.HTTPS);
        Context c = component.getContext().createChildContext();

        try {
            component.getDefaultHost().attach(new RowStoreApplication(c, config));
            component.start();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(100);
        formatter.setLeftPadding(2);
        formatter.printHelp("rowstore", options,true);
    }

    private static void configureLogging(String logLevel) {
        Level l = Level.toLevel(logLevel, Level.INFO);
        Configurator.setRootLevel(l);
        log.info("Log level set to " + l);
    }

    private static void out(String s) {
        System.out.println(s);
    }

}