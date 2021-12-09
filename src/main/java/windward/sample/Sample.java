package windward.sample;

import WindwardRestApi.Api.*;
import WindwardRestApi.Model.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;


public class Sample
{
    public static void main(String[] args) throws IOException, ApiException, InterruptedException {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        //initialize a new client with your restful address
        WindwardClient client = new WindwardClient("http://localhost:50548/");

        // getting the version info
        VersionInfo version = client.getVersion();
        System.out.println(version.toString());

        //create a list of data sources and add your data sources to the list
        List<DataSource> dsList = new ArrayList<DataSource>();
        String dataSourcePath = "..\\files\\Manufacturing.xml";
        File dsFile = new File(dataSourcePath);
        Xml_10DataSource xmlDataSource = new Xml_10DataSource("MANF_DATA_2009", dsFile.getAbsolutePath(), null);
        dsList.add(xmlDataSource);

        // initializing our template by passing the output format (pdf), the byte array of our template document,
        // and the format of the template document (docx)
        String templateFilePath = "..\\files\\Manufacturing.docx";
        File templateFile = new File(templateFilePath);
        byte[] templateData = Files.readAllBytes(templateFile.toPath());
        Template testTemplate = new Template(Template.OutputFormatEnum.PDF, templateData, Template.FormatEnum.DOCX);
        //added the datasources to the template
        testTemplate.setDatasources(dsList);

        //DOCUMENT

        //posting the template for processing
        Document testPostDoc = client.postDocument(testTemplate);

        //check the status of the document to make sure its done processing before trying to retrieve it
        while(true)
        {
            int docStatus = client.getDocumentStatus(testPostDoc.getGuid());
            if(docStatus != 302)
            {
                System.out.println("Not ready, status = " + docStatus);
                Thread.sleep(2000);
            }
            else
            {
                System.out.println("Ready, status = " + docStatus);
                break;
            }
        }

        // retrieve the generated document
        Document testGetDocument = client.getDocument(testPostDoc.getGuid());
        System.out.println("Successfully got document with guid "+ testGetDocument.getGuid());

        //METRICS

        //posting the metrics for processing
        Metrics testPostMetrics = client.postMetrics(testTemplate);
        System.out.println("Posting metrics with guid "+ testPostMetrics.getGuid());
        //check the status of the metrics to make sure its done processing before trying to retrieve it
        while(true)
        {
            int metricsStatus = client.getMetricsStatus(testPostMetrics.getGuid());
            if(metricsStatus != 302)
            {
                System.out.println("Not ready, status = " + metricsStatus);
                Thread.sleep(2000);
            }
            else
            {
                System.out.println("Ready, status = " + metricsStatus);
                break;
            }
        }

        // retrieve the generated metrics
        Metrics testGetMetrics = client.getMetrics(testPostMetrics.getGuid());
        System.out.println("Successfully got metrics with guid "+ testGetMetrics.getGuid());
        //TagTree

        //posting the tagtree for processing
        TagTree testPostTagTree = client.postTagTree(testTemplate);
        System.out.println("Posting tagtree with guid "+ testPostTagTree.getGuid());
        //check the status of the tagtree to make sure its done processing before trying to retrieve it
        while(true)
        {
            int tagTreeStatus = client.getTagTreeStatus(testPostTagTree.getGuid());
            if(tagTreeStatus != 302)
            {
                System.out.println("Not ready, status = " + tagTreeStatus);
                Thread.sleep(2000);
            }
            else
            {
                System.out.println("Ready, status = " + tagTreeStatus);
                break;
            }
        }

        // retrieve the generated tagtree
        TagTree testGetTagTree= client.getTagTree(testPostTagTree.getGuid());
        System.out.println("Successfully got tag tree with guid "+ testGetTagTree.getGuid());

        //DELETE the document, metrics and tagtree when your are done with them

        client.deleteDocument(testPostDoc.getGuid());
        System.out.println("DELETED DOCUMENT WITH GUID: "+testPostDoc.getGuid());

        client.deleteMetrics(testPostMetrics.getGuid());
        System.out.println("DELETED METRICS WITH GUID: "+testPostMetrics.getGuid());

        client.deleteTagTree(testPostTagTree.getGuid());
        System.out.println("DELETED TAGTREE WITH GUID: "+testPostTagTree.getGuid());

    }
}