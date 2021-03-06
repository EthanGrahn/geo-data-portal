package gov.usgs.cida.gdp.dataaccess;

import gov.usgs.cida.gdp.utilities.HTTPUtils;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * @author razoerb
 */
public class WmsHelper {

    public static String getCapabilities(String wmsURL) throws IOException {
        InputStream is = null;
        try {
            is = HTTPUtils.sendPacket(
                new URL(wmsURL + "?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetCapabilities"), "GET");
            String result = HTTPUtils.getStringFromInputStream(is);
            return result;
        } finally {
            if (is != null) is.close();
        }
        
    }
}
