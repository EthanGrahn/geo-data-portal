<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0" 
				xmlns:dc="http://purl.org/dc/elements/1.1/"
				xmlns:dct="http://purl.org/dc/terms/"
				xmlns:gco="http://www.isotc211.org/2005/gco"
				xmlns:gmd="http://www.isotc211.org/2005/gmd"
				xmlns:srv="http://www.isotc211.org/2005/srv">
	<!--xsl:output method="html" encoding="ISO-8859-1"/-->


	<xsl:template match="/results/*[local-name()='GetRecordsResponse']">
		<xsl:apply-templates select="./*[local-name()='SearchResults']"/>
	</xsl:template>
    
	<xsl:variable name="scienceBaseFeature">
		<xsl:choose>
			<xsl:when test="/results/@scienceBaseFeature">
				<xsl:value-of select="/results/@scienceBaseFeature" />
			</xsl:when>
			<xsl:otherwise>
				false
			</xsl:otherwise>
		</xsl:choose>
	</xsl:variable>
    
	<xsl:variable name="scienceBaseCoverage">
		<xsl:choose>
			<xsl:when test="/results/@scienceBaseCoverage">
				<xsl:value-of select="/results/@scienceBaseCoverage" />
			</xsl:when>
			<xsl:otherwise>
				false
			</xsl:otherwise>
		</xsl:choose>
	</xsl:variable>
    
	<xsl:variable name="pageUrl">
		<xsl:text>javascript:(GDPCSWClient.getRecords</xsl:text>
		<xsl:text>('</xsl:text>
	</xsl:variable>

	<xsl:template match="*[local-name()='SearchResults']">
        

        
		<xsl:variable name="start">
			<xsl:value-of select="../../request/@start"/>
		</xsl:variable>

		<!-- because GeoNetwork does not return nextRecord we have to do some calculation -->
		<xsl:variable name="next">
			<xsl:choose>
				<xsl:when test="@nextRecord">
					<xsl:value-of select="@nextRecord"/>
				</xsl:when>
				<xsl:otherwise>
					<xsl:choose>
						<xsl:when test="number(@numberOfRecordsMatched) >= (number($start) + number(@numberOfRecordsReturned))">
							<xsl:value-of select="number($start) + number(@numberOfRecordsReturned)"/>
						</xsl:when>
						<xsl:otherwise>
							<xsl:value-of select="0"/>
						</xsl:otherwise>
					</xsl:choose>
				</xsl:otherwise>
			</xsl:choose>
		</xsl:variable>
        
		<div class="captioneddiv">
            
			<!--xsl:if test="number(@numberOfRecordsMatched) > number(@numberOfRecordsReturned)"-->
			<!-- because ESRI GPT returns always numberOfRecordsMatched = 0 -->
			<xsl:if test="number(@numberOfRecordsReturned) > 0 and ($start > 1 or number($next) > 0)">
				<h3 id='prev_next' style="float:right;top: -2.5em;">
					<xsl:if test="$start > 1">
						<a>
							<xsl:attribute name="href">
								<xsl:value-of select="$pageUrl"/>
								<xsl:value-of select="number($start)-number(../../request/@maxrecords)"/>
								<xsl:text>'));</xsl:text>
								<xsl:text>javascript:(Dataset.createCSWResponseDialog());</xsl:text>
							</xsl:attribute>
							<xsl:text>&lt;&lt; previous</xsl:text>
						</a>
					</xsl:if>
					<xsl:text>  || </xsl:text>
					<xsl:if test="number($next) > 0 and (number(@nextRecord) &lt;= number(@numberOfRecordsMatched))">
						<a>
							<xsl:attribute name="href">
								<xsl:value-of select="$pageUrl"/>
								<xsl:value-of select="$next"/>
								<xsl:text>'));</xsl:text>
								<xsl:text>javascript:(Dataset.createCSWResponseDialog());</xsl:text>
							</xsl:attribute>
							<xsl:text>next &gt;&gt;</xsl:text>
						</a>
					</xsl:if>
				</h3>
			</xsl:if>

			<h3 id="total_records_returned">
				<xsl:text>Total records returned: </xsl:text>
				<xsl:choose>
					<xsl:when test="@nextRecord > 0">
						<xsl:value-of select="@nextRecord - @numberOfRecordsReturned"/>
						<xsl:text> - </xsl:text>
						<xsl:value-of select="@nextRecord - 1"/>
						<xsl:text> (of </xsl:text>
						<xsl:value-of select="@numberOfRecordsMatched"/>
						<xsl:text>)</xsl:text>
					</xsl:when>
					<xsl:otherwise>
						<xsl:value-of select="@numberOfRecordsReturned"/>
					</xsl:otherwise>
				</xsl:choose>
			</h3>
    
			<br/>
			<ol>
				<xsl:attribute name="start">
					<xsl:value-of select="$start"/>
				</xsl:attribute>

				<xsl:apply-templates select="./*[local-name()='SummaryRecord']"/>
				<xsl:apply-templates select="./gmd:MD_Metadata"/>
			</ol>
		</div>
	</xsl:template>

	<xsl:template match="*[local-name()='SummaryRecord']">
		<xsl:for-each select=".">
			<li>
				<strong>
					<xsl:text>Title: </xsl:text>
				</strong>
				<a>
					<xsl:attribute name="href">
						<xsl:text>javascript:(Dataset.selectDatasetById</xsl:text>
						<xsl:text>('</xsl:text>
						<xsl:value-of select="./dc:identifier"/>
						<xsl:text>','</xsl:text>
						<xsl:value-of select="./dc:title"/>
						<xsl:text>'))</xsl:text>
					</xsl:attribute>
                    
					<xsl:attribute name="class">
						<xsl:text>li-dataset</xsl:text>
					</xsl:attribute>
                    
					<xsl:choose>
						<xsl:when test="./dc:title">
							<xsl:apply-templates select="./dc:title[1]"/>
						</xsl:when>
						<xsl:otherwise>
							<xsl:text> ...</xsl:text>
						</xsl:otherwise>
					</xsl:choose>
				</a>
				<br/>
				<xsl:apply-templates select="./dct:abstract"/>
				<br/>
				<strong>
					<xsl:text>Keywords: </xsl:text>
				</strong>
				<xsl:for-each select="./dc:subject">
					<xsl:if test=".!=''">
						<xsl:if test="position() &gt; 1">, </xsl:if>
						<i>
							<xsl:value-of select="."/>
						</i>
					</xsl:if>
				</xsl:for-each>
				<br/>
				<a>
					<xsl:attribute name="href">
						<xsl:text>javascript:(GDPCSWClient.popupMetadataById</xsl:text>
						<xsl:text>('</xsl:text>
						<xsl:value-of select="./dc:identifier"/>
						<xsl:text>));</xsl:text>
						<xsl:text>javascript:(Dataset.createCSWResponseDialog('getrecordbyid'));</xsl:text>
					</xsl:attribute>
					<xsl:text>Full Record</xsl:text>
				</a>
				<hr />
			</li>
		</xsl:for-each>
	</xsl:template>

	<xsl:template match="dc:title">
		<xsl:choose>
			<xsl:when test=".!=''">
				<xsl:value-of select="."/>
			</xsl:when>
			<xsl:otherwise>
				<xsl:text> ...</xsl:text>
			</xsl:otherwise>
		</xsl:choose>
	</xsl:template>

	<xsl:template match="dct:abstract">
		<strong>
			<xsl:text>Abstract: </xsl:text>
		</strong>
		<xsl:value-of select="substring(.,1,250)"/>
		<xsl:if test="string-length(.) &gt; 250">
			<xsl:text> ...</xsl:text>
		</xsl:if>
	</xsl:template>

	<xsl:template match="gmd:MD_Metadata">
		<xsl:for-each select=".">
			<xsl:variable name="opendapServicesCount" select="count(./gmd:identificationInfo/srv:SV_ServiceIdentification/srv:serviceType/gco:LocalName[contains(.,'OPeNDAP')])" />
			<li>
				<strong>
					<xsl:text>Title: </xsl:text>
				</strong>
				<a>
					<xsl:attribute name="title">
						<xsl:text>Select A Data Set</xsl:text>
					</xsl:attribute>
                    
					<xsl:attribute name="class">
						<xsl:text>li-dataset</xsl:text>
					</xsl:attribute>
                    
					<xsl:choose>
						<!-- If we are in the context of searching for ScienceBase features, the 
						function we execute on click is not going to be the same as when we 
						are seatching for ScienceBase coverages -->
						<xsl:when test="$scienceBaseFeature = 'true'">
							<xsl:attribute name="href">
								<xsl:text>javascript:(ScienceBase.selectFeatureById</xsl:text>
								<xsl:text>('</xsl:text>
								<xsl:value-of select="./gmd:fileIdentifier/gco:CharacterString"/>
								<xsl:text>'))</xsl:text>
							</xsl:attribute>
						</xsl:when>
						<!-- Multiple OpenDAP services found. A secondary window to select from those
						is going to be necessary -->
						<xsl:when test="$opendapServicesCount &gt; 1">
							<xsl:attribute name="href">
								<xsl:text>javascript:(Dataset.displayMultipleOpenDAPSelection</xsl:text>
								<xsl:text>('</xsl:text>
								<xsl:value-of select="./gmd:fileIdentifier/gco:CharacterString"/>
								<xsl:text>'))</xsl:text>
							</xsl:attribute>
						</xsl:when>
						<xsl:when test="$scienceBaseCoverage = 'true'">
							<xsl:attribute name="href">
								<xsl:text>javascript:(Dataset.selectDatasetById</xsl:text>
								<xsl:text>('</xsl:text>
								<xsl:value-of select="./gmd:fileIdentifier/gco:CharacterString"/>
								<xsl:text>','</xsl:text>
								<xsl:value-of select="./gmd:identificationInfo/srv:SV_ServiceIdentification[@id='OGC-WCS']/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString"/>
								<xsl:text>'))</xsl:text>
							</xsl:attribute>
						</xsl:when>
						<xsl:otherwise>
							<xsl:attribute name="href">
								<xsl:text>javascript:(Dataset.selectDatasetById</xsl:text>
								<xsl:text>('</xsl:text>
								<xsl:value-of select="./gmd:fileIdentifier/gco:CharacterString"/>
								<xsl:text>','</xsl:text>
								<xsl:value-of select="./gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString"/>
								<xsl:text>'))</xsl:text>
							</xsl:attribute>
						</xsl:otherwise>
					</xsl:choose>
					<xsl:choose>
						<xsl:when test="./gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title[not(@gco:nilReason='missing')]/gco:CharacterString">
							<xsl:apply-templates select="(./gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title[not(@gco:nilReason='missing')]/gco:CharacterString)[1]"/>
						</xsl:when>
						<xsl:when test="./gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:title[not(@gco:nilReason='missing')]/gco:CharacterString">
							<xsl:apply-templates select="(./gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:title[not(@gco:nilReason='missing')]/gco:CharacterString)[1]"/>
						</xsl:when>
						<xsl:otherwise>
							<xsl:text> ...</xsl:text>
						</xsl:otherwise>
					</xsl:choose>
				</a>
				<br />
				<xsl:choose>
					<xsl:when test="./gmd:identificationInfo/srv:SV_ServiceIdentification[@id='OGC-WFS']/gmd:abstract/gco:CharacterString">
						<xsl:apply-templates select="./gmd:identificationInfo/srv:SV_ServiceIdentification[@id='OGC-WFS']/gmd:abstract/gco:CharacterString" />
					</xsl:when>
					<xsl:otherwise>
						<xsl:apply-templates select="./gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString" />
					</xsl:otherwise>
				</xsl:choose>
				<hr />
			</li>
		</xsl:for-each>
	</xsl:template>

	<xsl:template match="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title[not(@gco:nilReason='missing')]/gco:CharacterString">
		<xsl:choose>
			<xsl:when test=".!=''">
				<xsl:value-of select="."/>
			</xsl:when>
			<xsl:otherwise>
				<xsl:text> ...</xsl:text>
			</xsl:otherwise>
		</xsl:choose>
	</xsl:template>


	<xsl:template match="gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:title[not(@gco:nilReason='missing')]/gco:CharacterString">
		<xsl:choose>
			<xsl:when test=".!=''">
				<xsl:value-of select="."/>
			</xsl:when>
			<xsl:otherwise>
				<xsl:text> ...</xsl:text>
			</xsl:otherwise>
		</xsl:choose>
	</xsl:template>
    
	<xsl:template match="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString">
		<strong>
			<xsl:text>Abstract: </xsl:text>
		</strong>
		<xsl:value-of select="substring(.,1,500)"/>
		<xsl:if test="string-length(.) &gt; 500">
			<xsl:text> ...</xsl:text>
		</xsl:if>
		<br/>
		<a>
			<xsl:attribute name="href">
				<xsl:text>javascript:(GDPCSWClient.popupMetadataById</xsl:text>
				<xsl:text>('</xsl:text>
				<xsl:value-of select="../../../../gmd:fileIdentifier/gco:CharacterString"/>
				<xsl:text>')</xsl:text>
				<xsl:text>);</xsl:text>
				<xsl:text>javascript:(Dataset.createCSWResponseDialog('getrecordbyid'));</xsl:text>
			</xsl:attribute>
			<xsl:text>Full Record</xsl:text>
		</a>
	</xsl:template>
    
	<xsl:template match="gmd:identificationInfo/srv:SV_ServiceIdentification[@id='OGC-WFS']/gmd:abstract/gco:CharacterString">
		<strong>
			<xsl:text>Abstract: </xsl:text>
		</strong>
		<xsl:value-of select="substring(.,1,500)"/>
		<xsl:if test="string-length(.) &gt; 500">
			<xsl:text> ...</xsl:text>
		</xsl:if>
		<br/>
		<a>
			<xsl:attribute name="href">
				<xsl:text>javascript:(GDPCSWClient.popupMetadataById</xsl:text>
				<xsl:text>('</xsl:text>
				<xsl:value-of select="../../../../gmd:fileIdentifier/gco:CharacterString"/>
				<xsl:text>));</xsl:text>
				<xsl:text>javascript:(Dataset.createCSWResponseDialog('getrecordbyid'));</xsl:text>
			</xsl:attribute>
			<xsl:text>Full Record</xsl:text>
		</a>
	</xsl:template>

</xsl:stylesheet>
