

<jsp:include page="js/ext/ext.jsp">
    <jsp:param name="debug-qualifier" value="${param['debug-qualifier']}" />
</jsp:include>

<jsp:include page="js/openlayers/openlayers.jsp">
    <jsp:param name="isDevelopment" value="${param['debug-qualifier']}" />
</jsp:include>

<jsp:include page="js/geoext/geoext.jsp" >
    <jsp:param name="debug-qualifier" value="${param['debug-qualifier']}" />
</jsp:include>

<jsp:include page="js/log4javascript/log4javascript.jsp"/>
<script type="text/javascript" src="js/OnReady.js"></script>
