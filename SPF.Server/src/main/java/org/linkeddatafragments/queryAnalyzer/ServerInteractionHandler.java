/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.linkeddatafragments.queryAnalyzer;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;


public class ServerInteractionHandler {

    private final CloseableHttpAsyncClient httpAsyncClient = HttpAsyncClients.createDefault();

    public String getServerFamiliesMetaData() {

        
         
        String familiesMetaDataUrl = Config.getInstance().getMetadatapath();

        httpAsyncClient.start();
        HttpRequestBase request;
        request = new HttpGet(familiesMetaDataUrl);
        //TODO what happens if the head is fetched?     
        Future<HttpResponse> future = httpAsyncClient.execute(request, null);
        //TODO return the future instead of synchronizing here...
        HttpResponse response;
        String content = "";
        try {
            response = future.get();
            Path metaDatapath = Paths.get(Config.getInstance().getMetadatapath());
            //response.getEntity().getContent()
            /*  BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
            String line;
            while (null != (line = br.readLine())) {
                content.append(line);
            }*/
           // System.out.println("_________________HIII____________");
            StringWriter writer = new StringWriter();
            IOUtils.copy(response.getEntity().getContent(), writer, "UTF-8");
            content = writer.toString();
             //System.out.println("content==>" + content);
             try (BufferedWriter buffwriter = Files.newBufferedWriter(metaDatapath))
                {   
                   
                    buffwriter.write(content);
                  //  buffwriter.flush();
                    buffwriter.close();
                   // writer.flush();
                    writer.close();
                } catch (IOException ex) {
                      Logger.getLogger(FamiliesConfig.class.getName()).log(Level.SEVERE, null, ex);
                  }
           

            // System.out.println( "PartitionsConfig = getnumFamilies ===>" +config.getNumFamilies());
            // System.out.println( "PartitionsConfig =  getFamilies ===>" +c Files.co Files.co Files.copy(new URL(url).openStream(), Paths.get(destination));py(new URL(url).openStream(), Paths.get(destination));py(new URL(url).openStream(), Paths.get(destination));onfig.getFamilies().size());
        } catch (InterruptedException ex) {
            Logger.getLogger(ServerInteractionHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ExecutionException ex) {
            Logger.getLogger(ServerInteractionHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ServerInteractionHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (UnsupportedOperationException ex) {
            Logger.getLogger(ServerInteractionHandler.class.getName()).log(Level.SEVERE, null, ex);
        }

        return content;
    }

    public void getServerFamilyData(String name, String destination) {
        try {
            String url = Config.getInstance().getServer() + name;
            try (InputStream fileUrl = new URL(url).openStream()) {
                Files.copy(fileUrl, Paths.get(destination),StandardCopyOption.REPLACE_EXISTING);
                 fileUrl.close();
            }
            
        } catch (MalformedURLException ex) {
            
            Logger.getLogger(ServerInteractionHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            System.out.println("ServerInteractionHandler => getServerFamilyData ()");
            Logger.getLogger(ServerInteractionHandler.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
