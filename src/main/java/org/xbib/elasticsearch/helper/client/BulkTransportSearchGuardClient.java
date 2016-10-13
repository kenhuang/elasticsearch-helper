package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.common.GcMonitor;
import org.xbib.elasticsearch.plugin.helper.HelperPlugin;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;

/**
 * Created by Ken.Huang on 11/10/2016.
 */
public class BulkTransportSearchGuardClient extends BulkTransportClient {

    private static final ESLogger logger = ESLoggerFactory.getLogger(BulkTransportSearchGuardClient.class.getName());

    @Override
    protected void createClient(Settings settings) {
        if(this.client != null) {
            logger.warn("client is open, closing...", new Object[0]);
            this.client.close();
            this.client.threadPool().shutdown();
            logger.warn("client is closed", new Object[0]);
            this.client = null;
        }

        if(this.gcmon != null) {
            this.gcmon.close();
        }

        if(settings != null) {
            String version = System.getProperty("os.name") + " " + System.getProperty("java.vm.name") + " " + System.getProperty("java.vm.vendor") + " " + System.getProperty("java.runtime.version") + " " + System.getProperty("java.vm.version");
            logger.info("creating transport client on {} with effective settings {}", new Object[]{version, settings.getAsMap()});
            logger.info("before adding SearchGuardSSLPlugin in BulkTransportSearchGuardClient");
            TransportClient.Builder builder = TransportClient.builder()
                    .addPlugin(HelperPlugin.class)
                    ;

            // optional search guard support
            logger.info("before adding SearchGuardSSLPlugin in BulkTransportSearchGuardClient");
            if (settings.get("elasticsearch.searchguard") != null) {
                builder.addPlugin(SearchGuardSSLPlugin.class);
            }

            this.client = builder.settings(settings).build();
            this.gcmon = new GcMonitor(settings);
            this.ignoreBulkErrors = settings.getAsBoolean("ignoreBulkErrors", Boolean.valueOf(true)).booleanValue();
        }
    }
}
