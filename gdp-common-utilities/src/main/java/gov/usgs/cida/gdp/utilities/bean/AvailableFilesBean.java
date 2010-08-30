package gov.usgs.cida.gdp.utilities.bean;

import gov.usgs.cida.gdp.utilities.FileHelper;

import java.util.ArrayList;
import java.util.List;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("files")
public class AvailableFilesBean implements XmlResponse {

    @XStreamAlias("shapeSets")
    private List<ShapeFileSetBean> shapeSetList;
    @XStreamAlias("exampleFiles")
    private List<FilesBean> exampleFileList;
    @XStreamAlias("userFiles")
    private List<FilesBean> userFileList;

    static public AvailableFilesBean getAvailableFilesBean(String baseDirectory) throws IllegalArgumentException {
        return AvailableFilesBean.getAvailableFilesBean(baseDirectory, null);
    }

    /**
     * Create an AvailableFilesBean from a passed base directory.
     *
     * @param baseDirectory
     * @param userDirectory
     * @return
     */
    static public AvailableFilesBean getAvailableFilesBean(String baseDirectory, String userDirectory) throws IllegalArgumentException {
        if (baseDirectory == null) return null;
        if ("".equals(baseDirectory)) return null;

        AvailableFilesBean result = new AvailableFilesBean();
        List<FilesBean> allFilesBeanList = new ArrayList<FilesBean>();
        List<ShapeFileSetBean> allShapes = new ArrayList<ShapeFileSetBean>();
        String exampleDirectory = baseDirectory
                + "Sample_Files"
                + FileHelper.getSeparator();

        // Create the user file bean list (if calling method decides)
        if (userDirectory != null && !"".equals(userDirectory)) {
            allFilesBeanList = FilesBean.getFilesBeanSetList(exampleDirectory, userDirectory);
        } else {
            allFilesBeanList = FilesBean.getFilesBeanSetList(exampleDirectory, true);
        }


        for (FilesBean filesBean : allFilesBeanList) {
            ShapeFileSetBean sfsb = null;
            if (filesBean.getUserDirectory() != null) {
                result.getUserFileList().add(filesBean);
            } else {
                result.getExampleFileList().add(filesBean);
            }

            if ((sfsb = ShapeFileSetBean.getShapeFileSetBeanFromFilesBean(filesBean)) != null) {
                allShapes.add(sfsb);
            }
        }

        result.setShapeSetList(allShapes);
        return result;
    }

    public List<ShapeFileSetBean> getShapeSetList() {
        if (this.shapeSetList == null) {
            this.shapeSetList = new ArrayList<ShapeFileSetBean>();
        }
        return this.shapeSetList;
    }

    public void setShapeSetList(List<ShapeFileSetBean> shapeSetList) {
        this.shapeSetList = shapeSetList;
    }

    public List<FilesBean> getExampleFileList() {
        if (this.exampleFileList == null) {
            this.exampleFileList = new ArrayList<FilesBean>();
        }
        return this.exampleFileList;
    }

    public void setExampleFileList(List<FilesBean> exampleFileList) {
        this.exampleFileList = exampleFileList;
    }

    public List<FilesBean> getUserFileList() {
        if (this.userFileList == null) {
            this.userFileList = new ArrayList<FilesBean>();
        }
        return this.userFileList;
    }

    public void setUserFileList(List<FilesBean> userFileList) {
        this.userFileList = userFileList;
    }
}
