package gov.usgs.cida.gdp.wps.algorithm;

import gov.usgs.cida.gdp.coreprocessing.Delimiter;
import gov.usgs.cida.gdp.coreprocessing.analysis.grid.FeatureCoverageWeightedGridStatistics;
import gov.usgs.cida.gdp.coreprocessing.analysis.grid.FeatureCoverageWeightedGridStatistics.GroupBy;
import gov.usgs.cida.gdp.coreprocessing.analysis.grid.FeatureCoverageWeightedGridStatisticsWriter.Statistic;
import gov.usgs.cida.gdp.wps.algorithm.annotation.Algorithm;
import gov.usgs.cida.gdp.wps.algorithm.annotation.ComplexDataInput;
import gov.usgs.cida.gdp.wps.algorithm.annotation.ComplexDataOutput;
import gov.usgs.cida.gdp.wps.algorithm.annotation.LiteralDataInput;
import gov.usgs.cida.gdp.wps.algorithm.annotation.Process;
import gov.usgs.cida.gdp.wps.binding.CSVFileBinding;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.SchemaException;
import org.n52.wps.io.data.binding.complex.GTVectorDataBinding;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.ft.FeatureDataset;

import static gov.usgs.cida.gdp.wps.algorithm.annotation.LiteralDataInput.ENUM_COUNT;

/**
 *
 * @author tkunicki
 */
@Algorithm(title="Feature Weighted Grid Statistics", version="1.0.0")
public class FeatureWeightedGridStatisticsAlgorithm extends AbstractAnnotatedAlgorithm {

    private FeatureCollection featureCollection;
    private String featureAttributeName;
    private URI datasetURI;
    private String datasetId;
    private Date timeStart;
    private Date timeEnd;
    private List<Statistic> statistics;
    private GroupBy groupBy;
    private Delimiter delimiter;

    private File output;

    @ComplexDataInput(identifier=GDPAlgorithmUtil.INPUT_FEATURE_COLLECTION, binding=GTVectorDataBinding.class)
    public void setFeatureCollection(FeatureCollection featureCollection) {
        this.featureCollection = featureCollection;
    }

    @LiteralDataInput(identifier=GDPAlgorithmUtil.INPUT_FEATURE_ATTRIBUTE_NAME)
    public void setFeatureAttributeName(String featureAttributeName) {
        this.featureAttributeName = featureAttributeName;
    }

    @LiteralDataInput(identifier=GDPAlgorithmUtil.INPUT_DATASET_URI)
    public void setDatasetURI(URI datasetURI) {
        this.datasetURI = datasetURI;
    }

    @LiteralDataInput(identifier=GDPAlgorithmUtil.INPUT_DATASET_ID)
    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    @LiteralDataInput(identifier="TIME_START", minOccurs=0)
    public void setTimeStart(Date timeStart) {
        this.timeStart = timeStart;
    }

    @LiteralDataInput(identifier="TIME_END", minOccurs=0)
    public void setTimeEnd(Date timeEnd) {
        this.timeEnd = timeEnd;
    }

    @LiteralDataInput(identifier="STATISTICS", maxOccurs=ENUM_COUNT)
    public void setStatistics(List<Statistic> statistics) {
        this.statistics = statistics;
    }

    @LiteralDataInput(identifier="GROUP_BY")
    public void setGroupBy(GroupBy groupBy) {
        this.groupBy = groupBy;
    }

    @LiteralDataInput(identifier="DELIMITER")
    public void setDelimiter(Delimiter delimiter) {
        this.delimiter = delimiter;
    }

    @ComplexDataOutput(identifier="OUTPUT", binding=CSVFileBinding.class)
    public File getOutput() {
        return output;
    }

    @Process
    public void process() {

        FeatureDataset featureDataset = null;
        BufferedWriter writer = null;
        try {
            if (featureCollection.getSchema().getDescriptor(featureAttributeName) == null) {
                addError("Attribute " + featureAttributeName + " not found in feature collection");
                return;
            }

            GridDatatype gridDatatype = GDPAlgorithmUtil.generateGridDataType(
                    datasetURI,
                    datasetId,
                    featureCollection.getBounds());

            Range timeRange = GDPAlgorithmUtil.generateTimeRange(
                    gridDatatype,
                    timeStart,
                    timeEnd);

            output = File.createTempFile("gdp", ".csv");
            writer = new BufferedWriter(new FileWriter(output));

            FeatureCoverageWeightedGridStatistics.execute(
                    featureCollection,
                    featureAttributeName,
                    gridDatatype,
                    timeRange,
                    statistics == null || statistics.isEmpty() ? Arrays.asList(Statistic.values()) : statistics,
                    writer,
                    groupBy == null ? GroupBy.STATISTIC : groupBy,
                    delimiter == null ? Delimiter.COMMA : delimiter);

        } catch (InvalidRangeException e) {
            addError("Error subsetting gridded data :" + e.getMessage());
        } catch (IOException e) {
            addError("IO Error :" + e.getMessage());
        } catch (FactoryException e) {
            addError("Error initializing CRS factory:" + e.getMessage());
        } catch (TransformException e) {
            addError("Error attempting CRS transform:" + e.getMessage());
        } catch (SchemaException e) {
            addError("Error subsetting gridded data :" + e.getMessage());
        } catch (Exception e) {
            addError("General Error: " + e.getMessage());
        } finally {
            if (featureDataset != null) try { featureDataset.close(); } catch (IOException e) { }
            IOUtils.closeQuietly(writer);
        }
    }
	
}
