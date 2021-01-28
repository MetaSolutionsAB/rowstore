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

import org.apache.log4j.Logger;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.Server;
import org.restlet.data.Protocol;

import java.io.File;
import java.net.URI;

public class RowStoreApplicationStandalone extends Application {

    static Logger log = Logger.getLogger(RowStoreApplicationStandalone.class);

    public static void main(String[] args) {
        int port = 8282;
        URI configURI = null;

        if (args.length > 0) {
            configURI = new File(args[0]).toURI();
            if (!new File(configURI).exists()) {
                System.err.println("Configuration file not found: " + configURI);
                configURI = null;
            }
        }

        if (args.length > 1) {
            try {
                port = Integer.valueOf(args[1]);
            } catch (NumberFormatException nfe) {
                System.err.println(nfe.getMessage());
            }
        }

        if (configURI == null && System.getenv(RowStoreApplication.ENV_CONFIG_URI) == null) {
            System.out.println("RowStore - http://entrystore.org/rowstore/");
            System.out.println("");
            System.out.println("Usage: rowstore [path to configuration file] [listening port]");
            System.out.println("");
            System.out.println("Path to configuration file may be omitted only if environment variable ROWSTORE_CONFIG_URI is set to a URI. No other parameters must be provided if the configuration file is not provided as parameter.");
            System.out.println("Default listening port is " + port + ".");

            System.exit(1);
        }

        Component component = new Component();
        Server server = component.getServers().add(Protocol.HTTP, port);
        server.getContext().getParameters().add("useForwardedForHeader", "true");
        component.getClients().add(Protocol.HTTP);
        component.getClients().add(Protocol.HTTPS);
        Context c = component.getContext().createChildContext();

        try {
            component.getDefaultHost().attach(new RowStoreApplication(c, configURI));
            component.start();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}