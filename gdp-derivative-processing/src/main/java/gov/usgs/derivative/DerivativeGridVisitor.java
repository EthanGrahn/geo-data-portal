package gov.usgs.derivative;

import com.google.common.base.Joiner;
import gov.usgs.derivative.aparapi.AbstractGridKernel;
import gov.usgs.derivative.grid.GridVisitor;
import gov.usgs.derivative.time.TimeStepDescriptor;
import gov.usgs.cida.gdp.coreprocessing.analysis.grid.GridType;
import gov.usgs.cida.gdp.coreprocessing.analysis.grid.GridUtility;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.dataset.CoordinateAxis1DTime;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.GridDatatype;
import ucar.units.ConversionException;

/**
 *
 * @author tkunicki
 */
public abstract class DerivativeGridVisitor extends GridVisitor {
    
    private final static Logger LOGGER = LoggerFactory.getLogger(DerivativeGridVisitor.class);
    
    private int xCount;
    private int yCount;
    private int yxCount;
    
    private CoordinateAxis1DTime timeAxis;
    
    private int cal365HackOffset = 0;
    
    private int outputTimeStepCurrentIndex;
    
    private DerivativeValueDescriptor valueDescriptor;
    private TimeStepDescriptor timeStepDescriptor;
    private DerivativeNetCDFFile derivativeNetCDFFile;
    
    private boolean traverseContinue; 
    
    protected List<Number> missingValuesList;
    protected boolean[] missingValuesMask = null;
    
    protected Array outputArray;
    
    protected AbstractGridKernel kernel;
    
    private final boolean cal365Hack;
    
    protected String outputDir;
    
    public DerivativeGridVisitor() {
        this(false);
    }
    
    public DerivativeGridVisitor(boolean cal365Hack) {
        traverseContinue = true;
        this.cal365Hack = cal365Hack;
    }
    
    public final DerivativeValueDescriptor getValueDescriptor() {
        return valueDescriptor;
    }
    
    public final TimeStepDescriptor getTimeStepDescriptor() {
        return timeStepDescriptor;
    }
    
    public final AbstractGridKernel getKernel() {
        return kernel;
    }
    
    protected abstract String getOutputFilePath();
    
    protected String generateOutputFileBaseName(List<GridDatatype> gridDatatypeList) {
        return Joiner.on(".").join(
            "derivative",
            gridDatatypeList.get(0).getVariable().getName(),
            getValueDescriptor().getOutputName()
//                    Joiner.on(":").join(getValueDescriptor().getCoordinateValues()),
            );
    }
    
    protected abstract DerivativeValueDescriptor generateDerivativeValueDescriptor(List<GridDatatype> gridDatatypeList);
    
    protected abstract TimeStepDescriptor generateDerivativeTimeStepDescriptor(List<GridDatatype> gridDatatypeList);
    
    protected abstract AbstractGridKernel generateGridKernel(List<GridDatatype> gridDatatypeList) throws ConversionException, Exception;
    
    @Override
    public void traverseStart(List<GridDatatype> gridDatatypeList) {
        super.traverseStart(gridDatatypeList);
        
        if (!isValidGridDataTypeList(gridDatatypeList)) {
            LOGGER.error("Invalid GridDatatype(s) encountered for this instance of {}.", getClass().getName());
            traverseContinue = false;
            return;
        }
        
        // GRR. HACK...
        missingValuesList = new ArrayList<Number>(gridDatatypeList.size());
        for (int gridIndex = 0; gridIndex < gridDatatypeList.size(); ++gridIndex) {
            GridDatatype gdt = gridDatatypeList.get(gridIndex);
            Attribute att = gdt.findAttributeIgnoreCase("missing_value");
            if (att != null) {
                Number n = att.getNumericValue();
                if (n != null) {
                    missingValuesList.add(n);
                } else {
                    missingValuesList.add(Float.NaN);
                }
            } else {
                missingValuesList.add(Float.NaN);
            }
            
        }
        
        GridCoordSystem gridCoordSystem = gridDatatypeList.get(0).getCoordinateSystem();
        
        timeAxis = gridCoordSystem.getTimeAxis1D();
        
        xCount = GridUtility.getXAxisLength(gridCoordSystem);
        yCount = GridUtility.getYAxisLength(gridCoordSystem);
        yxCount = xCount* yCount;
      
        outputTimeStepCurrentIndex = -1;
        
        valueDescriptor = generateDerivativeValueDescriptor(gridDatatypeList);
        timeStepDescriptor = generateDerivativeTimeStepDescriptor(gridDatatypeList);
        try {
            derivativeNetCDFFile = new DerivativeNetCDFFile(
                    getOutputFilePath(),
                    generateOutputFileBaseName(gridDatatypeList),
                    valueDescriptor,
                    timeStepDescriptor);
            derivativeNetCDFFile.createOuputNetCDFFile(gridCoordSystem);
            
            
        } catch (IOException e) {
            LOGGER.error("Error creating output netcdf file", e);
            traverseContinue = false;
        } catch (InvalidRangeException e) {
            LOGGER.error("Error creating output netcdf file", e);
            traverseContinue = false;
        }
        
        try {
            kernel = generateGridKernel(gridDatatypeList);
        } catch (ConversionException e) {
            LOGGER.error("Error converting input grid units to threshold units", e);
            traverseContinue = false;
        } catch (Exception e) {
            // Bah, SimpleUnit.factoryWithExceptions just shows Exception?  double bah!
            LOGGER.error("Error parsing units for threshold",  e);
            traverseContinue = false;
        }
        
        LOGGER.info("Calculating derivatives for {} time steps", getTimeStepDescriptor().getOutputTimeStepCount());
    }
    
    @Override
    public boolean traverseContinue() {
        return traverseContinue ? super.traverseContinue() : false;
    }

    @Override
    public void traverseEnd() {
        try {
            if (outputTimeStepCurrentIndex > -1) {
                outputTimeStepEnd(outputTimeStepCurrentIndex);
                writeTimeStepData();
            }
            derivativeNetCDFFile.getNetCDFFile().close();
        } catch (IOException e) {
            LOGGER.error("Error writing output netcdf file", e);
        }
        kernel.dispose();
        kernel = null;
    }
    
    @Override
    public boolean tStart(int tIndex) {
        DateTime inputTimeStepDateTime = new DateTime(timeAxis.getTimeDate(tIndex));
        // HACK START
        if (cal365Hack && inputTimeStepDateTime.getMonthOfYear() == 2 && inputTimeStepDateTime.getDayOfMonth() == 29) {
            cal365HackOffset++;
        }
        inputTimeStepDateTime = inputTimeStepDateTime.plusDays(cal365HackOffset);
        // HACK END
        int outputTimeStepNextIndex = getTimeStepDescriptor().getOutputTimeStepIndex(inputTimeStepDateTime);
        if (outputTimeStepNextIndex != outputTimeStepCurrentIndex) {
            if (outputTimeStepCurrentIndex > -1) {
                outputTimeStepEnd(outputTimeStepCurrentIndex);
                writeTimeStepData();
                int arrayCount = yxCount * (valueDescriptor.isDerivativeCoordinateValid() ? valueDescriptor.getCoordinateValues().size() : 1);
                for (int arrayIndex = 0; arrayIndex < arrayCount; ++arrayIndex) {
                    outputArray.setObject(arrayIndex, 0);
                }
            } else {
                int[] shape = valueDescriptor.isDerivativeCoordinateValid() ?
                        new int[] { 1, valueDescriptor.getCoordinateValues().size(), yCount, xCount } :
                        new int[] { 1, yCount, xCount };
                outputArray =  Array.factory( valueDescriptor.getOutputDataType(), shape);
            }
            outputTimeStepCurrentIndex = outputTimeStepNextIndex;
            if (outputTimeStepCurrentIndex > -1 && outputTimeStepCurrentIndex < getTimeStepDescriptor().getOutputTimeStepCount()) {
                outputTimeStepStart(outputTimeStepCurrentIndex);
                LOGGER.debug("Start parsing data for {} to {}",
                            getTimeStepDescriptor().getOutputTimeStepLowerBound(outputTimeStepCurrentIndex).toString(),
                            getTimeStepDescriptor().getOutputTimeStepUpperBound(outputTimeStepCurrentIndex).toString());
            }
        }
        return outputTimeStepCurrentIndex > -1 || visitAllTimeSteps();
    }
    
    @Override
    public void yxStart(List<float[]> yxValuesList) {
        if (missingValuesMask == null) {
            missingValuesMask = DerivativeUtil.generateMissingValuesMask(yxValuesList, missingValuesList);
        }
        kernel.addYXInputValues(yxValuesList);
    }
 
    private void writeTimeStepData() {
        try {
            derivativeNetCDFFile.getNetCDFFile().write(
                    getValueDescriptor().getOutputName(),
                    new int[] { outputTimeStepCurrentIndex, 0, 0, 0},
                    outputArray);
            LOGGER.debug("Wrote data for {} to {}",
                        getTimeStepDescriptor().getOutputTimeStepLowerBound(outputTimeStepCurrentIndex).toString(),
                        getTimeStepDescriptor().getOutputTimeStepUpperBound(outputTimeStepCurrentIndex).toString());
        } catch (IOException e) {
            LOGGER.error("Error writing values to netcdf file", e);
        } catch (InvalidRangeException e) {
            LOGGER.error("Error writing values to netcdf file", e);
        }
    }
    
    public int getOutputCurrentTimeStep() {
        return outputTimeStepCurrentIndex;
    }
    
    protected void outputTimeStepStart(int outputTimeStepIndex) {}
    
    protected final void outputTimeStepEnd(int outputTimeStepIndex) {
        
        kernel.execute();
        LOGGER.debug("Kernel Execution: {} {}ms", kernel.getExecutionMode(), kernel.getExecutionTime());
        
        float[] zyxOutputValues = kernel.getZYXOutputValues();
        
        Number outputMissingValue = getValueDescriptor().getOutputMissingValue();
        int outputArrayIndex = 0;
        int zyxOutputIndex = 0;
        for (int zIndex = 0; zIndex < kernel.getZCount(); ++zIndex) {
            for (int yxIndex = 0; yxIndex < kernel.getYXCount(); ++yxIndex) {
                // TODO:  Warning, autobox/unbox, do we care about penalty?
                outputArray.setObject(
                        outputArrayIndex++,
                        // TODO:  Don't assume output MissingValue!
                        missingValuesMask[yxIndex] ? outputMissingValue : zyxOutputValues[zyxOutputIndex]);
                zyxOutputIndex++;
            }
            zyxOutputIndex += kernel.getYXPadding();
        }
    }
    
    // if false, only timesteps which overlap output timesteps will be passed.
    protected boolean visitAllTimeSteps() {
        return false;
    }
    
    protected abstract int getInputGridCount();
    
    protected abstract boolean isValidInputGridType(GridType gridType);
    
    protected final boolean isValidGridDataTypeList(List<GridDatatype> gridDataTypeList) {
        if (gridDataTypeList == null) {
            LOGGER.error("GridDatatype list may not be null");
            return false;
        }
        if (gridDataTypeList.size() != getInputGridCount()) {
            LOGGER.error("Invalid GridDatatype count for this visitor: expected {}, found {}", getInputGridCount(), gridDataTypeList.size());
            return false;
        }
        for (GridDatatype gridDatatype : gridDataTypeList) {
            if (gridDatatype == null) {
                LOGGER.error("GridDatatype list entry may not be null");
                return false;
            }
            GridType gridType = GridType.findGridType(gridDatatype.getCoordinateSystem());
            if (!isValidInputGridType(gridType)) {
                LOGGER.error("Invalid GridType for this visitor: {} ", gridType);
                return false;
            }
        }
        return true;
    }
}
