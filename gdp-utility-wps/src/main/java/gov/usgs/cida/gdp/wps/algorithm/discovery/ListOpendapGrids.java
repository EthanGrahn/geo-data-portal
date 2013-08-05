package gov.usgs.cida.gdp.wps.algorithm.discovery;

import com.google.common.base.Preconditions;
import gov.usgs.cida.gdp.dataaccess.bean.DataTypeCollection;
import gov.usgs.cida.gdp.dataaccess.helper.OpendapServerHelper;
import gov.usgs.cida.gdp.dataaccess.bean.Response;
import gov.usgs.cida.gdp.dataaccess.cache.RawXmlResponse;
import gov.usgs.cida.gdp.dataaccess.cache.ResponseCache;
import gov.usgs.cida.gdp.dataaccess.cache.ResponseCache.CacheIdentifier;
import static gov.usgs.cida.gdp.dataaccess.cache.ResponseCache.CacheIdentifier.CacheType.*;
import org.apache.commons.lang.StringUtils;
import org.n52.wps.algorithm.annotation.Algorithm;
import org.n52.wps.algorithm.annotation.ComplexDataOutput;
import org.n52.wps.algorithm.annotation.Execute;
import org.n52.wps.algorithm.annotation.LiteralDataInput;
import org.n52.wps.algorithm.annotation.LiteralDataOutput;
import org.n52.wps.server.AbstractAnnotatedAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author isuftin
 */
@Algorithm(
		version = "1.0.0",
		title = "List OpendDAP Grids",
		abstrakt = "Lists OpenDAP Grids")
public class ListOpendapGrids extends AbstractAnnotatedAlgorithm {

	private static final Logger LOGGER = LoggerFactory.getLogger(ListOpendapGrids.class);
	private static final String PARAM_CATALOG_URL = "catalog-url";
	private static final String PARAM_USE_CACHE = "allow-cached-response";
	private static final String PARAM_RESULT = "result";
	private static final String PARAM_RESULT_JSON = "result_as_json";
	private static final String PARAM_RESULT_XML = "result_as_xml";
	private String catalogURL;
	private boolean useCache = false; // optional parameter, set default...
	private Response response = null;

	@LiteralDataInput(
			identifier = PARAM_CATALOG_URL,
			minOccurs = 1,
			maxOccurs = 1)
	public void setCatalogURL(String catalogURL) {
		this.catalogURL = catalogURL;
	}

	@LiteralDataInput(
			identifier = PARAM_USE_CACHE,
			minOccurs = 0,
			defaultValue = "false")
	public void setUseCache(boolean useCache) {
		this.useCache = useCache;
	}

	@LiteralDataOutput(identifier = PARAM_RESULT)
	public String getResult() {
		return response.toXML();
	}

	@ComplexDataOutput(
			identifier = PARAM_RESULT_XML,
			binding = gov.usgs.cida.gdp.wps.binding.XMLBinding.class,
			abstrakt = "Returns XML")
	public String getResultAsXML() {
		return ((DataTypeCollection) response).toXML();
	}

	@ComplexDataOutput(
			identifier = PARAM_RESULT_JSON,
			binding = gov.usgs.cida.gdp.wps.binding.JSONBinding.class,
			abstrakt = "Returns JSON")
	public String getResultAsJSON() {
		return response.toJSON();
	}

	@Execute
	public void process() {
		Preconditions.checkArgument(StringUtils.isNotBlank(catalogURL), "Invalid " + PARAM_CATALOG_URL);

		CacheIdentifier cacheIdentifier = new ResponseCache.CacheIdentifier(
				catalogURL, DATA_TYPE, null);

		try {
			if (useCache && ResponseCache.hasCachedResponse(cacheIdentifier)) {
				this.response = DataTypeCollection.buildFromCache(cacheIdentifier);
			}

			if (this.response == null) {
				this.response = OpendapServerHelper.callDDSandDAS(catalogURL);
				if (this.response != null && !((DataTypeCollection) this.response).getDataTypeCollection().isEmpty() && useCache) {
					this.response.writeToCache(cacheIdentifier);
				}
			}
		} catch (Exception ex) {
			LOGGER.error(ex.getMessage());
			addError(ex.getMessage());
			throw new RuntimeException("An error has occured while processing response. Error: " + ex.getMessage(), ex);
		}
	}
}
