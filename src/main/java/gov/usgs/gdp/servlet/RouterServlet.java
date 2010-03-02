package gov.usgs.gdp.servlet;

import gov.usgs.gdp.bean.AckBean;
import gov.usgs.gdp.bean.ErrorBean;
import gov.usgs.gdp.bean.XmlReplyBean;

import com.sun.xml.fastinfoset.*; 

import java.io.IOException;
import java.io.Writer;
import java.util.Date;
import java.util.Map;
import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.log4j.Logger;

/**
 * Servlet implementation class RouterServlet
 */
public class RouterServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static org.apache.log4j.Logger log = Logger.getLogger(RouterServlet.class);

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		Long start = new Date().getTime();
		Map<String, String> requestParameters = request.getParameterMap();

                // If the user is attempting to upload files, send them directly to the correct servlet
		if (ServletFileUpload.isMultipartContent(request)) {
			log.info("User is attempting to upload files.");			
			RequestDispatcher rd = request.getRequestDispatcher("/UploadFilesServlet?command=upload");
			rd.forward(request, response);
			return;
		}
		
		if (!requestParameters.containsKey("command")) {
			log.info("User did not send command");
			ErrorBean errorBean = new ErrorBean(ErrorBean.ERR_NO_COMMAND);
			XmlReplyBean xmlReply = new XmlReplyBean(AckBean.ACK_FAIL, errorBean);
			RouterServlet.sendXml(xmlReply, start, response);
			return;
		}
		
		String command = request.getParameter("command");
		
		if ("listfiles".equals(command)) {
			log.info("User is attempting to list files");
            String userDirectory = request.getParameter("userdirectory");
            String userDirectoryCommand = "";
            if (userDirectory != null && !"".equals(userDirectory)) userDirectoryCommand = "&userdirectory=" + userDirectory;
			RequestDispatcher rd = request.getRequestDispatcher("/FileSelectionServlet?command=listfiles" + userDirectoryCommand);
			rd.forward(request, response);
			return;
		}
		
		if ("listattributes".equals(command)) {
			log.info("User is attempting to list attributes");
			String shapefile = request.getParameter("shapefile");
			RequestDispatcher rd = request.getRequestDispatcher("/FileAttributeServlet?command=listattributes&shapefile=" + shapefile);
			rd.forward(request, response);
			return;
		}
		
		if ("listfeatures".equals(command)) {
			log.info("User is attempting to list features");
			String shapefile = request.getParameter("shapefile");
			String attribute = request.getParameter("attribute");
			RequestDispatcher rd = request.getRequestDispatcher("/FileFeatureServlet?command=listfeatures&shapefile=" + shapefile + "&attribute=" + attribute);
			rd.forward(request, response);
			return;
		}
		
		if ("listthredds".equals(command)) {
			log.info("User is attempting to list THREDDS servers");
			RequestDispatcher rd = request.getRequestDispatcher("/THREDDSCheckServlet?command=listthredds");
			rd.forward(request, response);
			return;
		}
		
		if ("checkserver".equals(command)) {
            log.info("User is attempting to check server status");
            String hostname = request.getParameter("hostname");
            String port = request.getParameter("port");
            String uri = request.getParameter("uri");
            RequestDispatcher rd = request.getRequestDispatcher("/THREDDSCheckServlet?command=checkserver&hostname=" + hostname + "&port=" + port + "&uri=" + uri);
            rd.forward(request, response);
            return;
		}
		
		if ("createdatastore".equals(command)) {
			log.info("User is attempting to create data store");
            String userDirectory = request.getParameter("userdirectory");
			String shapefile = request.getParameter("shapefile");
			String datafile = request.getParameter("datafile");
			String attribute = request.getParameter("attribute");
			RequestDispatcher rd = request.getRequestDispatcher("/GeoServerServlet?command=createdatastore&shapefile=" + 
					shapefile + "&csvfile=" + datafile + "&attribute" + attribute + "&userdirectory" + userDirectory);
			rd.forward(request, response);
			return;
		}
		
		if ("getdatafileselectables".equals(command)) {
			log.info("User is attempting to get data file information");
			String datafile = request.getParameter("datafile");
			RequestDispatcher rd = request.getRequestDispatcher("/GeoServerServlet?command=getdatafileselectables" +
					"&csvfile=" + datafile);
			rd.forward(request, response);
			return;
		}
		
		if ("getdatasetlist".equals(command)) {
			log.info("User is attempting to list datasets");
			String hostname = request.getParameter("dataseturl");
			String port = request.getParameter("port");
			String uri = request.getParameter("uri");
			RequestDispatcher rd = request.getRequestDispatcher("/THREDDSServlet?command=getdatasetlist&hostname=" + hostname + "&port=" + port + "&uri=" + uri);
			rd.forward(request, response);
			return;
		}
		
		if ("getgridlist".equals(command)) {
			log.info("User is attempting to get a list of datatypes");
			String dataseturl = request.getParameter("dataseturl");
			RequestDispatcher rd = request.getRequestDispatcher("/THREDDSServlet?command=getgridlist&dataseturl=" + dataseturl);
			rd.forward(request, response);
			return;
		}
		
		if ("getcatalog".equals(command)) {
			log.info("User is attempting to grab catalog from remote THREDDS server");
			String url = request.getParameter("url");
			RequestDispatcher rd = request.getRequestDispatcher("/THREDDSServlet?command=getcatalog&url=" + url);
			rd.forward(request, response);
			return;
		}
		
		if ("gettimerange".equals(command)) {
			log.info("User is attempting to get a time range");
			String datasetUrl = request.getParameter("dataseturl");
			String gridSelection = request.getParameter("grid");
			RequestDispatcher rd = request.getRequestDispatcher("/THREDDSServlet?command=gettimerange&dataseturl=" + datasetUrl + "&grid=" + gridSelection);
			rd.forward(request, response);
			return;
		}
		
		if ("submitforprocessing".equals(command)) {
			log.info("User has submitted a job for processing.");
			String url = "";			
			String shapeSet = request.getParameter("shapeset");                        
			String attribute = request.getParameter("attribute");
			String[] features = request.getParameterValues("feature");
			String thredds = request.getParameter("thredds");
			String dataset = request.getParameter("dataset");                        
			String[] dataTypes = request.getParameterValues("datatype");                        
			String from = request.getParameter("from");                        
			String to = request.getParameter("to");                        
			String output = request.getParameter("outputtype");                        
			String email = request.getParameter("email");                        
			String outputFile = request.getParameter("outputfile");
			String userDirectory = request.getParameter("userdirectory");
			
			url = url + "&shapeset=" + shapeSet;
			url = url + "&attribute=" + attribute;
			for (String feature : features) url = url + "&feature=" + feature;
			url = url + "&thredds=" + thredds;
			url = url + "&dataset=" + dataset;
			for (String dataType : dataTypes) url = url + "&datatype=" + dataType;
			if (to != null && "".equals(to)) url = url + "&to=" + to;
			if (from != null && "".equals(from)) url = url + "&from=" + from;
			url = url + "&output=" + output;
			url = url + "&outputfile=" + outputFile;
			url = url + "&userdirectory=" + userDirectory;
			if (email != null && "".equals(email))url = url + "&email=" + email;
			RequestDispatcher rd = request.getRequestDispatcher("/FileProcessServlet?command=submitforprocessing");
			rd.forward(request, response);
			return;
		}
		
		if ("outputtypelist".equals(command)) {
			log.info("User is attempting to list the output types this application offers.");
			RequestDispatcher rd = request.getRequestDispatcher("/FileAttributeServlet?command=outputtypelist");
			rd.forward(request, response);
			return;
		}
		
		if ("getfile".equals(command)) {
			log.info("User is attempting to grab a file from the application.");
			String file = request.getParameter("file");
			RequestDispatcher rd = request.getRequestDispatcher("/FileProcessServlet?command=getfile&file=" + file);
			rd.forward(request, response);
			return;
		}
		
		if ("checkuploadfile".equals(command)) {
			log.info("User is checking to see if a file exists on the server.");
			String file = request.getParameter("file");
			RequestDispatcher rd = request.getRequestDispatcher("/FileProcessServlet?command=checkuploadfile&file=" + file);
			rd.forward(request, response);
		}

	}

	public static void sendXml(String xml,  Long startTime, HttpServletResponse response) throws IOException {
		log.debug(xml);
		Writer writer = response.getWriter();
		 try {
			response.setContentType("text/xml");
			response.setCharacterEncoding("utf-8");
			char[] characters = xml.toCharArray();
			for (int index = 0; index < characters.length; ++index) {
				char current = characters[index];
				if (DecoderStateTables.UTF8(current) == DecoderStateTables.STATE_ILLEGAL) {
					current = '\u00BF';
				}
				writer.write(current);
			}
			writer.flush();
		} finally {
			if (writer != null) writer.close();
		}
		log.info("Process completed in " + (new Date().getTime() - startTime) + " milliseconds.");
	}
	
	public static void sendXml(XmlReplyBean xmlReply, Long startTime, HttpServletResponse response) throws IOException {
		sendXml(xmlReply.toXml(), startTime, response);
	}

}
