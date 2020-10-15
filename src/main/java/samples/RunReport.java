/*
 * Copyright (c) 2020 by Windward Studios, Inc. All rights reserved.
 *
 * This program can be copied or used in any manner desired.
 */

package samples;

import WindwardRestApi.Api.WindwardClient;
import WindwardRestApi.Model.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.*;

/**
 * A sample usage of Windward Reports. This program generates reports based on the command line.
 * This project is used for two purposes, as a sample and as a way to run reports easily from the command line (mostly
 * for testing). <br/>
 * To get the parameters, the RunReport with no parameters and it will list them all out.
 */

public class RunReport {

	private static WindwardClient client;

	private static Logger log = Logger.getLogger(RunReport.class);

	/**
	 * Create a report using Windward Reports.
	 *
	 * @param args Run with no parameters to list out usage.
	 * @throws Throwable Thrown if anything goes wrong.
	 */
	public static void main(String[] args) throws Throwable {

		Properties props = new Properties();
		File file = new File("windwardreports.properties");
		InputStream is = new FileInputStream(file);
		props.load(is);
		String uri = props.getProperty("baseuri");
		System.out.println(String.format("Connecting to URL %s", uri));

		client = new WindwardClient(uri);
		VersionInfo version = client.getVersion();
		System.out.println(String.format("REST server version = %s", version));

		// if no arguments, then we list out the usage.
		if (args.length < 2) {
			DisplayUsage();
			return;
		}

		// parse the arguments passed in. This method makes no calls to Windward, it merely organizes the passed in arguments.
		CommandLine cmdLine = CommandLine.Factory(args);

		// the try here is so we can print out an exception if it is thrown. This code does minimal error checking and no other
		// exception handling to keep it simple & clear.
		try {
			// start elapsed after
			// init() as that only occurs on start-up and is not relevant in an app
			Date start = new Date();

			if (!cmdLine.isPerformance()) {
				PerfCounters perfCounters = runOneReport(cmdLine, args.length == 2);
				printPerformanceCounter(start, perfCounters, false);
			} else {
				new RunReport().runMultipleReports(cmdLine);
			}

		} catch (Throwable t) {
			log.debug("RunReport main()", t);
			System.err.println();
			System.err.println("Error: " + t.getMessage());
			System.err.println("Stack trace:");
			t.printStackTrace();
			throw t;
		}
	}

	private void runMultipleReports(CommandLine cmdLine) throws InterruptedException, IOException {

		File dirReports = new File(cmdLine.getReportFilename()).getAbsoluteFile().getParentFile();
		if (!dirReports.isDirectory()) {
			System.err.println("The directory " + dirReports.getAbsolutePath() + " does not exist");
			return;
		}

		// drop out threads - default is twice the number of cores.
		int numThreads = cmdLine.getNumThreads();
		numReportsRemaining = cmdLine.getNumReports();

		// run num threads
		ReportWorker[] th = new ReportWorker[numThreads];
		for (int ind = 0; ind < numThreads; ind++)
			th[ind] = new ReportWorker(ind, new CommandLine(cmdLine));

		DateFormat df = DateFormat.getTimeInstance(DateFormat.MEDIUM);
		Date startTime = new Date();
		System.out.println("Start time: " + df.format(startTime) + ", " + numThreads + " threads, " + cmdLine.getNumReports() + " reports");

		for (int ind = 0; ind < numThreads; ind++)
			th[ind].start();

		// we wait
		synchronized (this) {
			threadsRunning += numThreads;
			while (threadsRunning > 0)
				wait();
		}

		PerfCounters perfCounters = new PerfCounters();
		for (int ind = 0; ind < numThreads; ind++)
			perfCounters.add(th[ind].perfCounters);

		System.out.println();
		printPerformanceCounter(startTime, perfCounters, true);
	}

	private static void printPerformanceCounter(Date startTime, PerfCounters perfCounters, boolean multiThreaded) {

		Date endTime = new Date();
		DateFormat df = DateFormat.getTimeInstance(DateFormat.MEDIUM);

		long elapsed = endTime.getTime() - startTime.getTime();
		System.out.println("End time: " + df.format(endTime));
		System.out.println("Elapsed time: " + ticksAsTime(elapsed));
		System.out.println("Time per report: " + (perfCounters.numReports == 0 ? "n/a" : ticksAsTime(elapsed / perfCounters.numReports)));
		System.out.println("Pages/report: " + (perfCounters.numReports == 0 ? "n/a" : perfCounters.numPages / perfCounters.numReports));
		System.out.printf("Pages/sec: %.02f", (float) (perfCounters.numPages * 1000L) / (float) elapsed).println();
		if (multiThreaded)
			System.out.println("Below values are totaled across all threads (and so add up to more than the elapsed time)");
		System.out.println("  Generate: " + ticksAsTime(perfCounters.timeGenerate));
	}

	private static String ticksAsTime(long ticks) {

		int hours = (int) (ticks / (60 * 60 * 1000));
		ticks %= 60 * 60 * 1000;
		int minutes = (int) (ticks / (60 * 1000));
		ticks %= 60 * 1000;
		int seconds = (int) (ticks / 1000);
		ticks %= 1000;
		return twoDigits(hours) + ":" + twoDigits(minutes) + ":" + twoDigits(seconds) + "." + ticks;
	}

	private static String twoDigits(int time) {
		String rtn = Integer.toString(time);
		while (rtn.length() < 2)
			rtn = '0' + rtn;
		return rtn;
	}

	private int numReportsRemaining;

	private synchronized boolean hasNextReport() {
		numReportsRemaining--;
		return numReportsRemaining >= 0;
	}

	private int threadsRunning = 0;

	private synchronized void markDone() {
		threadsRunning--;
		notify();
	}

	private class ReportWorker extends Thread {
		private int threadNum;
		private CommandLine cmdLine;
		PerfCounters perfCounters;

		ReportWorker(int threadNum, CommandLine cmdLine) {
			this.threadNum = threadNum;
			this.cmdLine = cmdLine;
			perfCounters = new PerfCounters();
		}

		public void run() {

			try {
				while (hasNextReport()) {
					System.out.print("" + threadNum + '.');
					PerfCounters pc = runOneReport(cmdLine, false);
					perfCounters.add(pc);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				markDone();
			}
		}
	}

	private static PerfCounters runOneReport(CommandLine cmdLine, boolean preservePodFraming) throws Exception {

		Date start = new Date();
		PerfCounters perfCounters = new PerfCounters();

		// get the template and output file streams. Output is null for printers
		OutputStream reportOutput;

		if ((!cmdLine.getReportFilename().endsWith(".prn")) && (!cmdLine.getReportFilename().endsWith(".svg")) &&
				(!cmdLine.getReportFilename().endsWith(".eps")) && (!cmdLine.getReportFilename().endsWith(".bmp")) &&
				(!cmdLine.getReportFilename().endsWith(".gif")) && (!cmdLine.getReportFilename().endsWith(".jpg")) &&
				(!cmdLine.getReportFilename().endsWith(".png")) && (!cmdLine.getReportFilename().endsWith(".tif")) &&
				(!cmdLine.getReportFilename().endsWith(".jpeg")) && (!cmdLine.getReportFilename().endsWith(".tiff")))
			reportOutput = cmdLine.getOutputStream();
		else
			reportOutput = null;

		if (!cmdLine.isPerformance()) {
			System.out.println("Template: " + cmdLine.getTemplateFilename());
			System.out.println("Report: " + cmdLine.getReportFilename());
		}

		// Create the template object, based on the file extension
		Template.OutputFormatEnum formatOutput = Template.OutputFormatEnum.fromValue(FilenameUtils.getExtension(cmdLine.getReportFilename()));
		Template.FormatEnum formatTemplate = Template.FormatEnum.fromValue(FilenameUtils.getExtension(cmdLine.getTemplateFilename()));
		Template template = new Template(formatOutput, IOUtils.toByteArray(new FileInputStream(cmdLine.getTemplateFilename())), formatTemplate);

		template.setTrackErrors(cmdLine.verifyFlag);
		if (cmdLine.getLocale() != null)
			template.getProperties().add(new Property("report.locale", cmdLine.getLocale().toString()));

		for (Iterator<Map.Entry<String, Object>> it = cmdLine.getMap().entrySet().iterator(); it.hasNext(); ) {
			Map.Entry<String, Object> entry = it.next();
			template.getParameters().add(new Parameter(entry.getKey(), entry.getValue()));
		}

		// list out vars
		if (cmdLine.getNumDatasources() > 0)
			for (Map.Entry entry : cmdLine.getMap().entrySet())
				System.out.println(entry.getKey() + " = " + entry.getValue() + (entry.getValue() == null ? "" : " (" + entry.getValue().getClass().getName() + ")"));

		// Now for each datasource, we apply it to the report. This is complex because it handles all datasource types
		for (int ind = 0; ind < cmdLine.getNumDatasources(); ind++) {
			CommandLine.DatasourceInfo dsInfo = cmdLine.getDatasource(ind);

			// build the datasource
			switch (dsInfo.getType()) {

				case CommandLine.DatasourceInfo.TYPE_JSON:
					if (!cmdLine.isPerformance())
						System.out.println("JSON datasource: " + dsInfo.getFilename());
					template.getDatasources().add(new JsonDataSource(dsInfo.name, IOUtils.toByteArray(new FileInputStream(dsInfo.getExConnectionString()))));
					break;

				// An XML datasource.
				case CommandLine.DatasourceInfo.TYPE_XML_20:
					if (!cmdLine.isPerformance()) {
						if ((dsInfo.getSchemaFilename() == null) || (dsInfo.getSchemaFilename().length() == 0))
							System.out.println("XML (XPath 2.0) datasource: " + dsInfo.getFilename());
						else
							System.out.println("XML (XPath 2.0) datasource: " + dsInfo.getFilename() + ", schema " + dsInfo.getSchemaFilename());
					}

					if ((dsInfo.getSchemaFilename() == null) || (dsInfo.getSchemaFilename().length() == 0))
						template.getDatasources().add(new Xml_20DataSource(dsInfo.name,
								IOUtils.toByteArray(new FileInputStream(dsInfo.getExConnectionString())), null));
					else
						template.getDatasources().add(new Xml_20DataSource(dsInfo.name,
								IOUtils.toByteArray(new FileInputStream(dsInfo.getExConnectionString())),
								IOUtils.toByteArray(new FileInputStream(dsInfo.getSchemaFilename()))));
					break;

				// using the old dom4j XPath 1.0
				case CommandLine.DatasourceInfo.TYPE_XML_10:
					if (!cmdLine.isPerformance()) {
						if ((dsInfo.getSchemaFilename() == null) || (dsInfo.getSchemaFilename().length() == 0))
							System.out.println("XML (XPath 1.0) datasource: " + dsInfo.getFilename());
						else
							System.out.println("XML (XPath 1.0) datasource: " + dsInfo.getFilename() + ", schema " + dsInfo.getSchemaFilename());
					}

					if ((dsInfo.getSchemaFilename() == null) || (dsInfo.getSchemaFilename().length() == 0))
						template.getDatasources().add(new Xml_10DataSource(dsInfo.name,
								IOUtils.toByteArray(new FileInputStream(dsInfo.getExConnectionString())), null));
					else
						template.getDatasources().add(new Xml_10DataSource(dsInfo.name,
								IOUtils.toByteArray(new FileInputStream(dsInfo.getExConnectionString())),
								IOUtils.toByteArray(new FileInputStream(dsInfo.getSchemaFilename()))));
					break;

				// An OData datasource.
				case CommandLine.DatasourceInfo.TYPE_ODATA:
					if (!cmdLine.isPerformance())
						System.out.println("OData datasource: " + dsInfo.getFilename());

					template.getDatasources().add(new ODataDataSource(dsInfo.name, dsInfo.getExConnectionString()));
					break;

				//A SalesForce datsource.
				case CommandLine.DatasourceInfo.TYPE_SFORCE:
					if (!cmdLine.isPerformance())
						System.out.println("SalesForce datasource: " + dsInfo.getFilename());
					template.getDatasources().add(new SalesforceDataSource(dsInfo.name, dsInfo.getExConnectionString()));
					break;

				case CommandLine.DatasourceInfo.TYPE_SQL:
					if (!cmdLine.isPerformance())
						System.out.println(dsInfo.getSqlDriverInfo().getName() + " datasource: " + dsInfo.getExConnectionString());
					template.getDatasources().add(new SqlDataSource(dsInfo.name, dsInfo.getSqlDriverInfo().getClassname(), dsInfo.getExConnectionString()));
					break;
				default:
					throw new IllegalArgumentException("Unknown datasource type " + dsInfo.getType());
			}
		}

		if (!cmdLine.isPerformance())
			System.out.println("Calling REST engine to start generating report");

		// create the job
		Document document = client.postDocument(template);
		String guid = document.getGuid();

		if (!cmdLine.isPerformance())
			System.out.println(String.format("REST Engine has accepted job %s", guid));

		// wait for it to complete
		// instead of this you can use: template.setCallback("http://localhost/alldone/{guid}");
		while (client.getDocumentStatus(guid) != 302)
			Thread.sleep(100);

		// get the result
		document = client.getDocument(guid);

		// delete it off the server
		client.deleteDocument(guid);

		if (!cmdLine.isPerformance())
			System.out.println(String.format("REST Engine has completed job %s", document.getGuid()));

		perfCounters.timeGenerate = new Date().getTime() - start.getTime();
		perfCounters.numPages = document.getNumberOfPages();
		perfCounters.numReports = 1;

		printVerify(document);

		// save
		if (document.getData() != null) {
			FileOutputStream output = new FileOutputStream(new File(cmdLine.getReportFilename()));
			IOUtils.write(document.getData(), output);
			output.close();
		} else {
			String prefix = FilenameUtils.removeExtension(cmdLine.getReportFilename());
			String extension = FilenameUtils.getExtension(cmdLine.getReportFilename());
			for (int i = 0; i < document.getPages().length; ++i) {
				byte[] page = document.getPages()[i];
				String filename = prefix + "_" + Integer.toString(i) + "." + extension;
				FileOutputStream stream = new FileOutputStream(filename);
				stream.write(page);
				System.out.println("Bitmap page written to " + filename);
				stream.close();
			}
		}

		if (!cmdLine.isPerformance())
			System.out.println("Report complete, " + document.getNumberOfPages() + " pages long");

		if (cmdLine.isLaunch()) {
			String filename = cmdLine.getReportFilename();
			System.out.println("launching report " + filename);

			try {
				// java.awt.Desktop.getDesktop().open(new File(filename));
				Class classDesktop = Class.forName("java.awt.Desktop");
				Method method = classDesktop.getMethod("getDesktop", (Class[]) null);
				Object desktop = method.invoke(null, (Object[]) null);
				method = classDesktop.getMethod("open", File.class);
				method.invoke(desktop, new File(filename));

			} catch (Exception ex) {
				if (filename.indexOf(' ') != -1)
					filename = '"' + filename + '"';
				String ver = System.getProperty("os.name");
				if (ver.toLowerCase().indexOf("windows") != -1)
					Runtime.getRuntime().exec("rundll32 SHELL32.DLL,ShellExec_RunDLL " + filename);
				else
					Runtime.getRuntime().exec("open " + filename);
			}
		}

		return perfCounters;
	}

	private static void printVerify(Document report) {
		for (Issue issue : report.getErrors())
			System.err.println(issue.getMessage());
	}

	private static void DisplayUsage() {
		System.out.println("Windward Reports REST Engline C# Client API");
		System.out.println("usage: RunReport template_file output_file [-basedir path] [-xml xml_file | -sql connection_string | -oracle connection_string | -ole oledb_connection_string] [licenseKey=value | ...]");
		System.out.println("       The template file can be a docx, pptx, or xlsx file.");
		System.out.println("       The output file extension determines the report type created:");
		System.out.println("           output.csv - SpreadSheet CSV file");
		System.out.println("           output.docx - Word 2007+ DOCX file");
		System.out.println("           output.htm - HTML file with no CSS");
		System.out.println("           output.html - HTML file with CSS");
		System.out.println("           output.pdf - Acrobat PDF file");
		System.out.println("           output.pptx - PowerPoint 2007+ PPTX file");
		System.out.println("           output.prn - Printer where \"output\" is the printer name");
		System.out.println("           output.rtf - Rich Text Format file");
		System.out.println("           output.txt - Ascii text file");
		System.out.println("           output.xhtml - XHTML file with CSS");
		System.out.println("           output.xlsx - Excel 2007+ XLSX file");
		System.out.println("           output.xlsm - Excel 2007+ macro enabled XLSM file");
		System.out.println("       -basedir c:\\test - sets the datasource base directory to the specified folder (c:\\test in this example)");
		System.out.println("       -launch - will launch the report when complete.");
		System.out.println("       -performance:123 - will run the report 123 times.");
		System.out.println("            output file is used for directory and extension for reports");
		System.out.println("       -threads:4 - will create 4 threads when running -performance.");
		System.out.println("       -verify:N - turn on the error handling and verify feature where N is a number: 0 (none) , 1 (track errors), 2 (verify), 3 (all).  The list of issues is printed to the standard error.");
		System.out.println("       -version=9 - sets the template to the passed version (9 in this example)");
		System.out.println("       encoding=UTF-8 (or other) - set BEFORE datasource to specify an encoding");
		System.out.println("       locale=en_US - set the locale passed to the engine.");
		System.out.println("       pod=pod_filename - set a POD file (datasets)");
		System.out.println("       username=user password=pass - set BEFORE datasource for database connections");
		System.out.println("       The datasource is identified with a pair of parameters (the [prepend] part is prepended to the connection string");
		for (int ind = 0; ind < AdoDriverInfo.getDrivers().size(); ind++)
			System.out.println("           -" + AdoDriverInfo.getDrivers().get(ind).getName() + " connection_string - ex: " + AdoDriverInfo.getDrivers().get(ind).getExample());
		System.out.println("           -json filename - passes a JSON file as the datasource");
		System.out.println("                filename can be a url/filename or a connection string");
		System.out.println("           -odata url - passes a url as the datasource accessing it using the OData protocol");
		System.out.println("           -sforce - password should be password+securitytoken");
		System.out.println("           -xml filename - passes an xml file as the datasource");
		System.out.println("                -xml xmlFilename=schema:schemaFilename - passes an xml file and a schema file as the datasource");
		System.out.println("                filename can be a filename or a connection string");
		System.out.println("           -dom4j filename - passes an xml file as the datasource. Uses XPath 1.0 (dom4J)");
		System.out.println("                -dom4j xmlFilename=schema:schemaFilename - passes an xml file and a schema file as the datasource");
		System.out.println("                filename can be a filename or a connection string");
		System.out.println("           -[xml|sql|...]:name names this datasource with name");
		System.out.println("                     must come BEFORE each -xml, -sql, ... part");
		System.out.println("       You can have 0-N key=value pairs that are passed to the datasource Map property");
		System.out.println("            If the value starts with I', F', or D' it parses it as an integer, float, or date(yyyy-MM-ddThh:mm:ss)");
		System.out.println("                example  date=\"D'1996-08-29\"");
		System.out.println("            If the value is * it will set a filter of all");
		System.out.println("            If the value is \\\"text,text,...\\\" it will set a filter of all");
	}

	/**
	 * This class contains everything passed in the command line. It makes no calls to Windward Reports.
	 */
	private static class CommandLine {
		private String templateFilename;
		private String reportFilename;
		private Map<String, Object> map;
		private List<DatasourceInfo> datasources;
		private Map<String, DataSource> dataProviders;
		private Locale locale;
		private boolean launch;
		private int templateVersion;
		private int numReports;
		private int writeTags;
		private int numThreads;
		private int dataMode;
		private String dataFileName;
		private String baseDirectory;
		private int verifyFlag;
		private byte[] templateFile;

		/**
		 * Create the object.
		 *
		 * @param templateFilename The name of the template file.
		 * @param reportFilename   The name of the report file. null for printer reports.
		 */
		public CommandLine(String templateFilename, String reportFilename) {
			this.templateFilename = GetFullPath(templateFilename);
			if (!reportFilename.toLowerCase().endsWith(".prn"))
				reportFilename = GetFullPath(reportFilename);
			launch = false;
			this.reportFilename = reportFilename;
			map = new HashMap<String, Object>();
			datasources = new ArrayList<DatasourceInfo>();
			writeTags = -1;
			numThreads = Runtime.getRuntime().availableProcessors() * 2;
			verifyFlag = 0;
		}

		private static String GetFullPath(String filename) {
			int pos = filename.indexOf(':');
			if ((pos == -1) || (pos == 1))
				return new File(filename).getAbsolutePath();
			return filename;
		}

		public CommandLine(CommandLine src) {
			templateFilename = src.templateFilename;
			reportFilename = src.reportFilename;
			map = src.map == null ? null : new HashMap<String, Object>(src.map);
			datasources = src.datasources == null ? null : new ArrayList<DatasourceInfo>(src.datasources);
			dataProviders = src.dataProviders == null ? null : new HashMap<String, DataSource>();
			locale = src.locale;
			launch = src.launch;
			templateVersion = src.templateVersion;
			numReports = src.numReports;
			writeTags = src.writeTags;
			numThreads = src.numThreads;
			dataMode = src.dataMode;
			dataFileName = src.dataFileName;
			baseDirectory = src.baseDirectory;
			verifyFlag = src.verifyFlag;
			templateFile = src.templateFile;
		}

		/**
		 * The name of the template file.
		 *
		 * @return The name of the template file.
		 */
		public String getTemplateFilename() {
			return templateFilename;
		}

		public InputStream getTemplateStream() throws IOException {

			int pos = templateFilename.indexOf(':');
			if ((pos != -1) && (pos != 1))
				return new URL(templateFilename).openStream();
			return new FileInputStream(templateFilename);
		}

		/**
		 * The name of the report file. null for printer reports.
		 *
		 * @return The name of the report file. null for printer reports.
		 */
		public String getReportFilename() {
			return reportFilename;
		}

		public OutputStream getOutputStream() throws IOException {
			if (!isPerformance())
				return new FileOutputStream(reportFilename);
			File dirReports = new File(reportFilename).getAbsoluteFile().getParentFile();
			String extReport = reportFilename.substring(reportFilename.lastIndexOf('.'));
			String filename = File.createTempFile("rpt_", extReport, dirReports).getAbsolutePath();
			return new FileOutputStream(filename);
		}

		/**
		 * The parameters passed for each datasource to be created.
		 *
		 * @return The parameters passed for each datasource to be created.
		 */
		public List<DatasourceInfo> getDatasources() {
			return datasources;
		}

		/**
		 * The parameters passed for each datasource to be created.
		 *
		 * @param datasources The parameters passed for each datasource to be created.
		 */
		public void setDatasources(List<DatasourceInfo> datasources) {
			this.datasources = datasources;
		}

		/**
		 * If we are caching the data providers, this is them for passes 1 .. N (set on pass 0)
		 */
		public Map<String, DataSource> getDataProviders() {
			return dataProviders;
		}

		/**
		 * If we are caching the data providers, this is them for passes 1 .. N (set on pass 0)
		 */
		public void setDataProviders(Map<String, DataSource> dataProviders) {
			this.dataProviders = dataProviders;
		}

		/**
		 * true if launch the app at the end.
		 *
		 * @return true if launch the app at the end.
		 */
		public boolean isLaunch() {
			return launch;
		}

		/**
		 * true if launch the app at the end.
		 *
		 * @param launch true if launch the app at the end.
		 */
		public void setLaunch(boolean launch) {
			this.launch = launch;
		}

		/**
		 * The template version number. 0 if not set.
		 *
		 * @return The template version number.
		 */
		public int getTemplateVersion() {
			return templateVersion;
		}

		/**
		 * The template version number. 0 if not set.
		 *
		 * @param templateVersion The template version number.
		 */
		public void setTemplateVersion(int templateVersion) {
			this.templateVersion = templateVersion;
		}

		/**
		 * The ProcessReportAPI.TAG_STYLE_* to write unhandled tags with.
		 *
		 * @param tagStyle ProcessReportAPI.TAG_STYLE_*
		 */
		public void setWriteTags(int tagStyle) {
			this.writeTags = tagStyle;
		}

		/**
		 * Gets the ProcessReportAPI.TAG_STYLE_* to write unhandled tags with, or -1 if not set.
		 *
		 * @return ProcessReportAPI.TAG_STYLE_* or -1 if not set
		 */
		public int getWriteTags() {
			return writeTags;
		}

		/**
		 * The name/value pairs for variables passed to the datasources. key is a String and value is a String,
		 * Number, or Date.
		 *
		 * @return The name/value pairs for variables passed to the datasources.
		 */
		public Map<String, Object> getMap() {
			return map;
		}

		/**
		 * The number of datasources.
		 *
		 * @return The number of datasources.
		 */
		public int getNumDatasources() {
			return datasources.size();
		}

		/**
		 * The parameters passed for each datasource to be created.
		 *
		 * @param index The datasource to return.
		 * @return The parameters for the datasource to be created.
		 */
		public DatasourceInfo getDatasource(int index) {
			return datasources.get(index);
		}

		/**
		 * The locale to run under.
		 *
		 * @return The locale to run under.
		 */
		public Locale getLocale() {
			return locale;
		}

		/**
		 * For performance modeling, how many reports to run.
		 *
		 * @return How many reports to run.
		 */
		public int getNumReports() {
			return numReports;
		}

		/**
		 * true if requesting a performance run
		 *
		 * @return true if requesting a performance run
		 */
		public boolean isPerformance() {
			return numReports != 0;
		}

		/**
		 * The number of threads to launch if running a performance test.
		 *
		 * @return The number of threads to launch if running a performance test.
		 */
		public int getNumThreads() {
			return numThreads;
		}

		/**
		 * The data mode for this report. Controls the generation of a data.xml file.
		 *
		 * @return The data mode for this report.
		 */
		public int getDataMode() {
			return dataMode;
		}

		/**
		 * If the data.xml is to be written to an external file, this is the file.
		 *
		 * @return If the data.xml is to be written to an external file, this is the file.
		 */
		public String getDataFileName() {
			return dataFileName;
		}

		public String getBaseDirectory() {
			return baseDirectory;
		}

		public boolean isBaseDirectorySet() {
			return baseDirectory != null;
		}

		int getVerifyFlag() {
			return verifyFlag;
		}

		/**
		 * The parameters passed for a single datasource. All filenames are expanded to full paths so that if an exception is
		 * thrown you know exactly where the file is.
		 */
		private static class DatasourceInfo {

			/**
			 * A SQL database.
			 */
			public static final int TYPE_SQL = 1;

			/**
			 * An XML file.
			 */
			public static final int TYPE_XML_20 = 2;

			/**
			 * An OData url.
			 */
			public static final int TYPE_ODATA = 3;

			/**
			 * JSON data source.
			 */
			public static final int TYPE_JSON = 5;

			/**
			 * SalesForce dat source.
			 */
			public static final int TYPE_SFORCE = 6;

			/**
			 * An XML file using dom4j (XPath 1.0)
			 */
			public static final int TYPE_XML_10 = 7;

			public static final int TYPE_DATASET = 8;

			private int type;
			private String name;

			private String filename;
			private String schemaFilename;

			private AdoDriverInfo sqlDriverInfo;
			private String connectionString;

			private String username;
			private String password;
			private String podFilename;

			private String encoding;
			private boolean restful;

			/**
			 * Create the object for a PLAYBACK datasource.
			 *
			 * @param filename The playback filename.
			 * @param type     What type of datasource.
			 */
			public DatasourceInfo(String filename, int type) {
				this.type = type;
				this.filename = filename;
			}

			/**
			 * Create the object for a XML datasource.
			 *
			 * @param name           The name for this datasource.
			 * @param filename       The XML filename.
			 * @param schemaFilename The XML schema filename. null if no schema.
			 * @param username       The username if credentials are needed to access the datasource.
			 * @param password       The password if credentials are needed to access the datasource.
			 * @param podFilename    The POD filename if datasets are being passed.
			 * @param type           What type of datasource.
			 */
			public DatasourceInfo(String name, String filename, String schemaFilename, String username, String password, String podFilename, int type) {
				this.name = name == null ? "" : name;
				this.filename = GetFullPath(filename);
				if ((schemaFilename != null) && (schemaFilename.length() > 0))
					this.schemaFilename = GetFullPath(schemaFilename);
				this.username = username;
				this.password = password;
				if ((podFilename != null) && (podFilename.length() > 0))
					this.podFilename = GetFullPath(podFilename);
				this.type = type;
			}

			/**
			 * Create the object for an OData datasource.
			 *
			 * @param name        The name for this datasource.
			 * @param url         the url for the service.
			 * @param username    The username if credentials are needed to access the datasource.
			 * @param password    The password if credentials are needed to access the datasource.
			 * @param podFilename The POD filename if datasets are being passed.
			 * @param type        What type of datasource.
			 */
			public DatasourceInfo(String name, String url, String username, String password, String podFilename, int type) {
				this.name = name == null ? "" : name;
				this.filename = url;
				this.username = username;
				this.password = password;
				if ((podFilename != null) && (podFilename.length() > 0))
					this.podFilename = GetFullPath(podFilename);
				this.type = type;
			}

			/**
			 * Create the object for a JSON datasource.
			 *
			 * @param name        The name for this datasource.
			 * @param url         the url for the service.
			 * @param username    The username if credentials are needed to access the datasource.
			 * @param password    The password if credentials are needed to access the datasource.
			 * @param podFilename The POD filename if datasets are being passed.
			 * @param type        What type of datasource.
			 */
			public DatasourceInfo(String name, String url, String schema, String username, String password, String podFilename, String encoding, int type) {
				this.name = name == null ? "" : name;
				this.filename = url;
				this.username = username;
				this.password = password;
				if ((podFilename != null) && (podFilename.length() > 0))
					this.podFilename = GetFullPath(podFilename);
				this.encoding = encoding;
				this.type = type;
			}

			/**
			 * Create the object for a SQL datasource.
			 *
			 * @param name             The name for this datasource.
			 * @param sqlDriverInfo    The DriverInfo for the selected SQL vendor.
			 * @param connectionString The connection string to connect to the database.
			 * @param username         The username if credentials are needed to access the datasource.
			 * @param password         The password if credentials are needed to access the datasource.
			 * @param podFilename      The POD filename if datasets are being passed.
			 * @param type             What type of datasource.
			 */
			public DatasourceInfo(String name, AdoDriverInfo sqlDriverInfo, String connectionString, String username, String password, String podFilename, int type) {
				this.name = name;
				this.sqlDriverInfo = sqlDriverInfo;
				this.connectionString = connectionString;
				this.username = username;
				this.password = password;
				if ((podFilename != null) && (podFilename.length() > 0))
					this.podFilename = GetFullPath(podFilename);
				this.type = type;
			}

			/**
			 * What type of datasource.
			 *
			 * @return What type of datasource.
			 */
			public int getType() {
				return type;
			}

			/**
			 * The name for this datasource.
			 *
			 * @return The name for this datasource.
			 */
			public String getName() {
				return name;
			}

			/**
			 * The XML filename.
			 *
			 * @return The XML filename.
			 */
			public String getFilename() {
				return filename;
			}

			/**
			 * The XML schema filename. null if no schema.
			 *
			 * @return The XML schema filename. null if no schema.
			 */
			public String getSchemaFilename() {
				return schemaFilename;
			}

			/**
			 * The DriverInfo for the selected SQL vendor.
			 *
			 * @return The DriverInfo for the selected SQL vendor.
			 */
			public AdoDriverInfo getSqlDriverInfo() {
				return sqlDriverInfo;
			}

			/**
			 * The connection string to connect to the database.
			 *
			 * @return The connection string to connect to the database.
			 */
			public String getConnectionString() {
				return connectionString;
			}

			/**
			 * The username if credentials are needed to access the datasource.
			 *
			 * @return The username if credentials are needed to access the datasource.
			 */
			public String getUsername() {
				return username;
			}

			/**
			 * The password if credentials are needed to access the datasource.
			 *
			 * @return The password if credentials are needed to access the datasource.
			 */
			public String getPassword() {
				return password;
			}

			/**
			 * The POD filename if datasets are being passed.
			 *
			 * @return The POD filename if datasets are being passed.
			 */
			public String getPodFilename() {
				return podFilename;
			}

			/**
			 * The JSON encoding
			 */
			public String getEncoding() {
				return encoding;
			}

			/**
			 * The connection string the new way for XML, etc.
			 */
			public String getExConnectionString() {
				return filename;
			}
		}

		/**
		 * Create a CommandLine object from the command line passed to the program.
		 *
		 * @param args The arguments passed to the program.
		 * @return A CommandLine object populated from the args.
		 */
		public static CommandLine Factory(String[] args) {

			CommandLine rtn = new CommandLine(args[0], args[1]);

			String username = null, password = null, podFilename = null, encoding = null;

			for (int ind = 2; ind < args.length; ind++) {
				int pos = args[ind].indexOf(':');
				String name = pos == -1 ? "" : args[ind].substring(pos + 1);
				String cmd = pos == -1 ? args[ind] : args[ind].substring(0, pos);

				if (cmd.equals("-performance")) {
					rtn.numReports = Integer.parseInt(name);
					continue;
				}

				if (cmd.equals("-threads")) {
					rtn.numThreads = Integer.parseInt(name);
					continue;
				}

				if (cmd.equals("-verify")) {
					rtn.verifyFlag = Integer.parseInt(name);
					continue;
				}

				if (cmd.equals("-launch")) {
					rtn.setLaunch(true);
					continue;
				}

				if (cmd.equals("-basedir")) {
					rtn.baseDirectory = args[++ind];
					continue;
				}

				if (cmd.equals("-rest")) {
					if (rtn.datasources.size() > 0)
						rtn.datasources.get(rtn.datasources.size() - 1).restful = true;
					continue;
				}

				if (cmd.equals("-xml") || cmd.equals("-dom4j")) {
					String xmlFilename = args[++ind];
					int split = xmlFilename.indexOf("=schema:");
					String schemaFilename;
					if (split == -1)
						schemaFilename = null;
					else {
						schemaFilename = xmlFilename.substring(split + 8).trim();
						xmlFilename = xmlFilename.substring(0, split).trim();
					}
					DatasourceInfo datasourceOn = new DatasourceInfo(name, xmlFilename, schemaFilename, username, password, podFilename,
							cmd.equals("-dom4j") ? DatasourceInfo.TYPE_XML_10 : DatasourceInfo.TYPE_XML_20);
					rtn.datasources.add(datasourceOn);
					username = password = podFilename = null;
					continue;
				}

				if (cmd.equals("-json")) {
					String url = args[++ind];
					DatasourceInfo datasourceOn = new DatasourceInfo(name, url, null, username, password, podFilename, encoding, DatasourceInfo.TYPE_JSON);
					rtn.datasources.add(datasourceOn);
					username = password = podFilename = null;
					continue;
				}

				if (cmd.equals("-odata")) {
					String url = args[++ind];
					DatasourceInfo datasourceOn = new DatasourceInfo(name, url, username, password, podFilename, DatasourceInfo.TYPE_ODATA);
					rtn.datasources.add(datasourceOn);
					username = password = podFilename = null;
					continue;
				}

				if (cmd.equals("-sforce")) {
					String url = "https://login.salesforce.com";
					DatasourceInfo datasourceOn = new DatasourceInfo(name, url, null, username, password, podFilename, DatasourceInfo.TYPE_SFORCE);
					rtn.datasources.add(datasourceOn);
					username = password = podFilename = null;
					continue;
				}

				if (cmd.equals("-dataset")) {
					String dataSetStr = args[++ind];
					DatasourceInfo dsInfo = new DatasourceInfo(name, dataSetStr, null, null, null, DatasourceInfo.TYPE_DATASET);
					rtn.datasources.add(dsInfo);
					username = password = podFilename = null;
					continue;
				}
				boolean isDb = false;
				for (int index = 0; index < AdoDriverInfo.getDrivers().size(); index++) {
					AdoDriverInfo di = AdoDriverInfo.getDrivers().get(index);
					if (cmd.equals("-" + di.getName())) {
						DatasourceInfo datasourceOn = new DatasourceInfo(name, di, args[++ind], username, password, podFilename, DatasourceInfo.TYPE_SQL);
						rtn.datasources.add(datasourceOn);
						isDb = true;
						username = password = podFilename = null;
						break;
					}
				}
				if (isDb)
					continue;

				// assume this is a key=value
				int equ = args[ind].indexOf('=');
				if (equ == -1)
					throw new IllegalArgumentException("Unknown option " + args[ind]);
				String key = args[ind].substring(0, equ);
				String value = args[ind].substring(equ + 1);

				// locale is global
				if (key.equals("locale")) {
					rtn.locale = new Locale(value.substring(0, 2), value.substring(3));
					continue;
				}
				if (key.equals("version")) {
					rtn.setTemplateVersion(Integer.parseInt(value));
					continue;
				}

				if (key.equals("username")) {
					username = value;
					continue;
				}
				if (key.equals("password")) {
					password = value;
					continue;
				}
				if (key.equals("pod")) {
					podFilename = value;
					continue;
				}
				if (key.equals("encoding")) {
					encoding = value;
					continue;
				}

				Object val;
				// may be a list
				if (value.indexOf(',') != -1) {
					val = new ArrayList();
					StringTokenizer tok = new StringTokenizer(value, ",", false);
					while (tok.hasMoreTokens()) {
						String elem = tok.nextToken();
						((ArrayList) val).add(convertValue(elem));
					}
				} else
					val = convertValue(value);
				rtn.map.put(key, val);
			}
			return rtn;
		}

		private static Object convertValue(String value) {

			if (value.startsWith("I'"))
				return Long.valueOf(value.substring(2));
			if (value.startsWith("F'"))
				return Double.valueOf(value.substring(2));
			if (value.startsWith("D'")) {
				ParsePosition pp = new ParsePosition(0);
				SimpleDateFormat stdFmt = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
				Date date = stdFmt.parse(value.substring(2), pp);
				if ((date != null) && (pp.getIndex() > 0))
					return date.toInstant().atOffset(ZoneOffset.UTC);
				;
				stdFmt = new SimpleDateFormat("yyyy-MM-dd");
				date = stdFmt.parse(value.substring(2), pp);
				if ((date != null) && (pp.getIndex() > 0))
					return date.toInstant().atOffset(ZoneOffset.UTC);
				;
				throw new IllegalArgumentException("Could not parse yyyy-MM-dd[Thh:mm:ss] date from " + value.substring(2));
			}
			return value.replace("\\n", "\n").replace("\\t", "\t");
		}
	}

	private static class PerfCounters {
		long timeGenerate;
		int numReports;
		int numPages;

		public void add(PerfCounters pc) {
			timeGenerate += pc.timeGenerate;
			numReports += pc.numReports;
			numPages += pc.numPages;
		}
	}

	/**
	 * Information on all known JDBC connectors.
	 */
	private static class AdoDriverInfo {
		private String name;
		private String classname;
		private String example;

		/**
		 * Create the object for a given vendor.
		 *
		 * @param name      The -vendor part in the command line (ex: -sql).
		 * @param classname The driver classname.
		 * @param example   A sample commandline.
		 */
		public AdoDriverInfo(String name, String classname, String example) {
			this.name = name;
			this.classname = classname;
			this.example = example;
		}

		/**
		 * The -vendor part in the command line (ex: -sql).
		 *
		 * @return The -vendor part in the command line (ex: -sql).
		 */
		public String getName() {
			return name;
		}

		/**
		 * The driver classname.
		 *
		 * @return The driver classname.
		 */
		public String getClassname() {
			return classname;
		}

		/**
		 * A sample commandline.
		 *
		 * @return A sample commandline.
		 */
		public String getExample() {
			return example;
		}

		private static List<AdoDriverInfo> listProviders;

		private static List<AdoDriverInfo> getDrivers() {

			if (listProviders != null)
				return listProviders;
			listProviders = new ArrayList<>();

			listProviders.add(new AdoDriverInfo("db2", "IBM.Data.DB2", "server=db2.windwardreports.com;database=SAMPLE;Uid=demo;Pwd=demo;"));
			listProviders.add(new AdoDriverInfo("mysql", "MySql.Data.MySqlClient", "server=mysql.windwardreports.com;database=sakila;user id=demo;password=demo;"));
			listProviders.add(new AdoDriverInfo("odbc", "System.Data.Odbc", "Driver={Sql Server};Server=localhost;Database=Northwind;User ID=test;Password=pass;"));
			listProviders.add(new AdoDriverInfo("oledb", "System.Data.OleDb", "Provider=sqloledb;Data Source=localhost;Initial Catalog=Northwind;User ID=test;Password=pass;"));
			listProviders.add(new AdoDriverInfo("oracle", "Oracle.ManagedDataAccess.Client", "Data Source=oracle.windwardreports.com:1521/HR;Persist Security Info=True;Password=HR;User ID=HR"));
			listProviders.add(new AdoDriverInfo("sql", "System.Data.SqlClient", "Data Source=mssql.windwardreports.com;Initial Catalog=Northwind;user=demo;password=demo;"));
			listProviders.add(new AdoDriverInfo("redshift", "Npgsql", "HOST=localhost;DATABASE=pagila;USER ID=test;PASSWORD=test;"));
			listProviders.add(new AdoDriverInfo("postgresql", "Npgsql", "HOST=localhost;DATABASE=pagila;USER ID=test;PASSWORD=test;"));

			// sort in name order
			Collections.sort(listProviders, Comparator.comparing(AdoDriverInfo::getName));

			return listProviders;
		}
	}
}