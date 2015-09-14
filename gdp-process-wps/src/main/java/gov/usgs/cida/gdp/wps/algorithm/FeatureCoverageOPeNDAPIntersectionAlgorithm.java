package gov.usgs.cida.gdp.wps.algorithm;

import gov.usgs.cida.gdp.constants.AppConstant;
import gov.usgs.cida.gdp.utilities.GeoTiffUtils;
import gov.usgs.cida.gdp.utilities.exception.GeoTiffUtilException;
import gov.usgs.cida.gdp.wps.algorithm.heuristic.ResultSizeAlgorithmHeuristic;
import gov.usgs.cida.gdp.wps.algorithm.heuristic.exception.AlgorithmHeuristicException;
import gov.usgs.cida.gdp.wps.binding.CoverageFileBinding;
import gov.usgs.cida.gdp.wps.binding.GMLStreamingFeatureCollectionBinding;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.List;

import org.geotools.feature.FeatureCollection;
import org.n52.wps.algorithm.annotation.Algorithm;
import org.n52.wps.algorithm.annotation.ComplexDataInput;
import org.n52.wps.algorithm.annotation.ComplexDataOutput;
import org.n52.wps.algorithm.annotation.Execute;
import org.n52.wps.algorithm.annotation.LiteralDataInput;
import org.n52.wps.server.AbstractAnnotatedAlgorithm;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import ucar.ma2.InvalidRangeException;
import ucar.nc2.dt.GridDataset;

/**
 *
 * @author tkunicki
 */
@Algorithm(
    version = "1.1.0",
    title = "OPeNDAP Subset",
    abstrakt="This service returns the subset of data that intersects a set of vector polygon features and time range, if specified. A NetCDF file will be returned.")
public class FeatureCoverageOPeNDAPIntersectionAlgorithm extends AbstractAnnotatedAlgorithm {
    
    private static final Logger log = LoggerFactory.getLogger(FeatureCoverageOPeNDAPIntersectionAlgorithm.class);

    public enum OutputType {
        netcdf,
        geotiff;
    }

    private ResultSizeAlgorithmHeuristic resultSizeHeuristic = new ResultSizeAlgorithmHeuristic();
    
    private FeatureCollection<?, ?> featureCollection;
    private URI datasetURI;
    private List<String> datasetId;
    private boolean requireFullCoverage;
    private Date timeStart;
    private Date timeEnd;
    private OutputType outputType;

    private File output;

    @ComplexDataInput(
            identifier=GDPAlgorithmConstants.FEATURE_COLLECTION_IDENTIFIER,
            title=GDPAlgorithmConstants.FEATURE_COLLECTION_TITLE,
            abstrakt=GDPAlgorithmConstants.FEATURE_COLLECTION_ABSTRACT,
            binding=GMLStreamingFeatureCollectionBinding.class)
    public void setFeatureCollection(FeatureCollection<?, ?> featureCollection) {
        this.featureCollection = featureCollection;
        this.resultSizeHeuristic.setFeatureCollection(featureCollection);
    }

    @LiteralDataInput(
            identifier=GDPAlgorithmConstants.DATASET_URI_IDENTIFIER,
            title=GDPAlgorithmConstants.DATASET_URI_TITLE,
            abstrakt=GDPAlgorithmConstants.DATASET_URI_ABSTRACT + " The data web service must adhere to the OPeNDAP protocol.")
    public void setDatasetURI(URI datasetURI) {
        this.datasetURI = datasetURI;
    }

    @LiteralDataInput(
            identifier=GDPAlgorithmConstants.DATASET_ID_IDENTIFIER,
            title=GDPAlgorithmConstants.DATASET_ID_TITLE,
            abstrakt=GDPAlgorithmConstants.DATASET_ID_ABSTRACT + " The data variable must be a gridded time series.",
            maxOccurs= Integer.MAX_VALUE)
    public void setDatasetId(List<String> datasetId) {
        this.datasetId = datasetId;
        this.resultSizeHeuristic.setGridVariableList(datasetId);
    }
    
    @LiteralDataInput(
            identifier=GDPAlgorithmConstants.REQUIRE_FULL_COVERAGE_IDENTIFIER,
            title=GDPAlgorithmConstants.REQUIRE_FULL_COVERAGE_TITLE,
            abstrakt=GDPAlgorithmConstants.REQUIRE_FULL_COVERAGE_ABSTRACT,
            defaultValue="true")
    public void setRequireFullCoverage(boolean requireFullCoverage) {
        this.requireFullCoverage = requireFullCoverage;
        this.resultSizeHeuristic.setRequireFullCoverage(requireFullCoverage);
    }
    
    @LiteralDataInput(
            identifier=GDPAlgorithmConstants.TIME_START_IDENTIFIER,
            title=GDPAlgorithmConstants.TIME_START_TITLE,
            abstrakt=GDPAlgorithmConstants.TIME_START_ABSTRACT,
            minOccurs=0)
    public void setTimeStart(Date timeStart) {
        this.timeStart = timeStart;
        this.resultSizeHeuristic.setDateTimeStart(timeStart);
    }

    @LiteralDataInput(
        identifier=GDPAlgorithmConstants.TIME_END_IDENTIFIER,
        title=GDPAlgorithmConstants.TIME_END_TITLE,
        abstrakt=GDPAlgorithmConstants.TIME_END_ABSTRACT,
        minOccurs=0)
    public void setTimeEnd(Date timeEnd) {
        this.timeEnd = timeEnd;
        this.resultSizeHeuristic.setDateTimeEnd(timeEnd);
    }
    
    @LiteralDataInput(
            identifier=GDPAlgorithmConstants.OUTPUT_TYPE_IDENTIFIER,
            title=GDPAlgorithmConstants.OUTPUT_TYPE_TITLE,
            abstrakt=GDPAlgorithmConstants.OUTPUT_TYPE_ABSTRACT,
            minOccurs=0,
            maxOccurs=1)
    public void setOutputType(OutputType outputType) {
        if (outputType == null) {
            this.outputType = OutputType.netcdf;
        } else {
            this.outputType = outputType;
        }
    }
    
    /*
     *  This is a bit confusing, but similar to FeatureWeightedGridStatistics
     *  if OUTPUT_TYPE is geotiff, the mimeType of the output should be
     *  application/zip, or the download will have the wrong extension
     *  if OUTPUT_TYPE is netcdf, the mimeType should be application/netcdf
     */
    @ComplexDataOutput(identifier="OUTPUT",
            title="Output File",
            abstrakt="A NetCDF file containing requested data.",
            binding=CoverageFileBinding.class)
    public File getOutput() {
        return output;
    }

    @Execute
    public void process() {
        GridDataset gridDataSet = null;
        try { 
            gridDataSet = GDPAlgorithmUtil.generateGridDataSet(datasetURI);
            
            /*
             * Lets run our size heuristic to see if we should go ahead and process
             * this request.
             */
            resultSizeHeuristic.setGridDataset(gridDataSet);
            if(!resultSizeHeuristic.validated()) {
            	/*
            	 * Create OPeNDAP URI and place in response message
            	 */
            	log.error(resultSizeHeuristic.getError());
            	addError(resultSizeHeuristic.getError());
            	return;
            }
            
            /*
             * Here we need to check what type of output the request is asking for.
             * If its a NetCDF file, we do the NetCDFGridWriter logic.
             * If its a GeoTiff file, we do the GeoTiffUtils logic.
             *      If no output is described we default to NetCDF
             */
            if(OutputType.geotiff == outputType) {
                output = GeoTiffUtils.generateGeoTiffZipFromGrid(gridDataSet, datasetId, featureCollection, requireFullCoverage, timeStart, timeEnd, AppConstant.WORK_LOCATION.getValue());
            } else {
                output = File.createTempFile(getClass().getSimpleName(), ".nc", new File(AppConstant.WORK_LOCATION.getValue()));
                NetCDFGridWriter.makeFile(
                        output.getAbsolutePath(),
                        gridDataSet,
                        datasetId,
                        featureCollection,
                        timeStart,
                        timeEnd,
                        requireFullCoverage,
                        "Grid sub-setted by USGS/CIDA Geo Data Portal");
            }
        } catch (InvalidRangeException e) {
            log.error("Error subsetting gridded data: ", e);
            addError("Error subsetting gridded data: " + e.getMessage());
        } catch (IOException e) {
            log.error("IO Error :", e);
            addError("IO Error :" + e.getMessage());
        } catch (FactoryException e) {
            log.error("Error initializing CRS factory: ", e);
            addError("Error initializing CRS factory: " + e.getMessage());
        } catch (TransformException e) {
            log.error("Error attempting CRS transform: ", e);
            addError("Error attempting CRS transform: " + e.getMessage());
        } catch (AlgorithmHeuristicException e) {
            log.error("Heuristic Error: ", e);
            addError("Heuristic Error: " + e.getMessage());
        } catch (GeoTiffUtilException e) {
            log.error("GeoTiff Generation Error: ", e);
            addError("GeoTiff Generation Error: " + e.getMessage());
        } catch (Exception e) {
            log.error("General Error: ", e);
            addError("General Error: " + e.getMessage());
        } finally {
            if (gridDataSet != null) try { gridDataSet.close(); } catch (IOException e) { }
        }
    }

}
