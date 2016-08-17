package gov.usgs.cida.gdp.wps.algorithm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.SchemaException;
import org.n52.wps.algorithm.annotation.Algorithm;
import org.n52.wps.algorithm.annotation.ComplexDataInput;
import org.n52.wps.algorithm.annotation.ComplexDataOutput;
import org.n52.wps.algorithm.annotation.Execute;
import org.n52.wps.algorithm.annotation.LiteralDataInput;
import org.n52.wps.server.AbstractAnnotatedAlgorithm;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import gov.usgs.cida.gdp.constants.AppConstant;
import gov.usgs.cida.gdp.coreprocessing.Delimiter;
import gov.usgs.cida.gdp.coreprocessing.analysis.grid.FeatureCategoricalGridCoverage;
import gov.usgs.cida.gdp.wps.binding.CSVFileBinding;
import gov.usgs.cida.gdp.wps.binding.GMLStreamingFeatureCollectionBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.dt.GridDatatype;

/**
 *
 * @author tkunicki
 */
@Algorithm(
    version="1.0.0",
    title="Categorical Coverage Fraction",
    abstrakt="This processing service is used with categorical gridded data to assess the percent coverage of each category for a set of features. This service does not process gridded time series. Using the feature dataset bounding-box, a subset of the gridded dataset is requested from the remote gridded data server. The location of each grid-cell center is then projected to the feature dataset coordinate reference system. For each grid-cell in the subsetted grid, the grid-cell center is tested for inclusion in each feature in the feature dataset. If the grid-cell center is in a given feature, the count for that cell's category is incremented for that feature. After all the grid-cell centers are processed the coverage fraction for each category is calculated for each feature.")
public class FeatureCategoricalGridCoverageAlgorithm extends AbstractAnnotatedAlgorithm {
    private static final Logger log = LoggerFactory.getLogger(FeatureCategoricalGridCoverageAlgorithm.class);
    private FeatureCollection<SimpleFeatureType,SimpleFeature> featureCollection;
    private String featureAttributeName;
    private URI datasetURI;
    private List<String> datasetId;
    private boolean requireFullCoverage = true;
    private Delimiter delimiter;

    private File output;

    @ComplexDataInput(
            identifier=GDPAlgorithmConstants.FEATURE_COLLECTION_IDENTIFIER,
            title=GDPAlgorithmConstants.FEATURE_COLLECTION_TITLE,
            abstrakt=GDPAlgorithmConstants.FEATURE_COLLECTION_ABSTRACT,
            binding=GMLStreamingFeatureCollectionBinding.class)
    public void setFeatureCollection(FeatureCollection<SimpleFeatureType,SimpleFeature> featureCollection) {
        this.featureCollection = featureCollection;
    }

    @LiteralDataInput(
            identifier=GDPAlgorithmConstants.FEATURE_ATTRIBUTE_NAME_IDENTIFIER,
            title=GDPAlgorithmConstants.FEATURE_ATTRIBUTE_NAME_TITLE,
            abstrakt=GDPAlgorithmConstants.FEATURE_ATTRIBUTE_NAME_ABSTRACT,
            minOccurs= 1,
            maxOccurs= 1)
    public void setFeatureAttributeName(String featureAttributeName) {
        if (null == featureAttributeName) {
            log.debug("WPS request process error: Feature Attribute Name was null. Can not process without the output column name.");
        }
        this.featureAttributeName = featureAttributeName;
    }

    @LiteralDataInput(
            identifier=GDPAlgorithmConstants.DATASET_URI_IDENTIFIER,
            title=GDPAlgorithmConstants.DATASET_URI_TITLE,
            abstrakt=GDPAlgorithmConstants.DATASET_URI_ABSTRACT,
            minOccurs= 1,            
            maxOccurs= 1)
    public void setDatasetURI(URI datasetURI) {
        this.datasetURI = datasetURI;
    }

    @LiteralDataInput(
            identifier=GDPAlgorithmConstants.DATASET_ID_IDENTIFIER,
            title=GDPAlgorithmConstants.DATASET_ID_TITLE,
            abstrakt=GDPAlgorithmConstants.DATASET_ID_ABSTRACT + " The data variable must be categorical in nature.",
            minOccurs= 1,            
            maxOccurs= Integer.MAX_VALUE)
    public void setDatasetId(List<String> datasetId) {
        this.datasetId = datasetId;
    }
    
    @LiteralDataInput(
            identifier=GDPAlgorithmConstants.REQUIRE_FULL_COVERAGE_IDENTIFIER,
            title=GDPAlgorithmConstants.REQUIRE_FULL_COVERAGE_TITLE,
            abstrakt=GDPAlgorithmConstants.REQUIRE_FULL_COVERAGE_ABSTRACT,
            defaultValue="true")
    public void setRequireFullCoverage(boolean requireFullCoverage) {
        this.requireFullCoverage = requireFullCoverage;
    }

    @LiteralDataInput(
        identifier=GDPAlgorithmConstants.DELIMITER_IDENTIFIER,
        title=GDPAlgorithmConstants.DELIMITER_TITLE,
        abstrakt=GDPAlgorithmConstants.DELIMITER_ABSTRACT,
        defaultValue="COMMA")
    public void setDelimiter(Delimiter delimiter) {
        this.delimiter = delimiter;
    }

    @ComplexDataOutput(identifier="OUTPUT",
            title="Output File",
            abstrakt="A delimited text file containing requested process output.",
            binding=CSVFileBinding.class)
    public File getOutput() {
        return output;
    }

    @Execute
    public void process() {
//        FeatureDataset featureDataset = null;
        BufferedWriter writer = null;

        try {
            String extension = (delimiter == null) ? Delimiter.getDefault().extension : delimiter.extension;
            output = File.createTempFile(getClass().getSimpleName(), extension, new File(AppConstant.WORK_LOCATION.getValue()));
            writer = new BufferedWriter(new FileWriter(output));

            for (String currentDatasetId : datasetId) {
                GridDatatype gridDatatype = GDPAlgorithmUtil.generateGridDataType(
                        datasetURI,
                        currentDatasetId,
                        featureCollection.getBounds(), requireFullCoverage);
                
                writer.write("# " + currentDatasetId);
                writer.newLine();
                FeatureCategoricalGridCoverage.execute(
                        featureCollection,
                        featureAttributeName,
                        gridDatatype,
                        writer,
                        delimiter == null ? Delimiter.getDefault() : delimiter,
                        requireFullCoverage);
            }

        } catch (InvalidRangeException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (FactoryException e) {
            throw new RuntimeException(e);
        } catch (TransformException e) {
            throw new RuntimeException(e);
        } catch (SchemaException e) {
            throw new RuntimeException(e);
        } finally {
//            if (featureDataset != null) try { featureDataset.close(); } catch (IOException e) { }
            IOUtils.closeQuietly(writer);
        }
    }
    
}
