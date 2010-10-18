package org.lilycms.tools.mavenplugin.kauridepresolver;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.factory.ArtifactFactory;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.resolver.ArtifactResolver;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class KauriProjectClasspath {
    protected XPathFactory xpathFactory = XPathFactory.newInstance();
    private String confDirectory;
    private String kauriVersion;
    private ArtifactFilter filter;
    private ArtifactFactory artifactFactory;
    protected ArtifactResolver resolver;
    protected List remoteRepositories;
    protected ArtifactRepository localRepository;
    private Log log;

    public KauriProjectClasspath(String confDirectory, String kauriVersion, Log log, ArtifactFilter filter,
            ArtifactFactory artifactFactory, ArtifactResolver resolver, List remoteRepositories,
            ArtifactRepository localRepository) {
        this.confDirectory = confDirectory;
        this.kauriVersion = kauriVersion;
        this.filter = filter;
        this.artifactFactory = artifactFactory;
        this.resolver = resolver;
        this.remoteRepositories = remoteRepositories;
        this.localRepository = localRepository;
        this.log = log;
    }

    public Set<Artifact> getAllArtifacts() throws MojoExecutionException {
        Set<Artifact> moduleArtifacts = getModuleArtifactsFromKauriConfig();

        Set<Artifact> result = new HashSet<Artifact>();
        result.addAll(moduleArtifacts);

        for (Artifact moduleArtifact : moduleArtifacts) {
            result.addAll(getClassPathArtifacts(moduleArtifact));
        }

        return result;
    }

    public Set<Artifact> getModuleArtifactsFromKauriConfig() throws MojoExecutionException {
        File configFile = new File(confDirectory, "kauri/wiring.xml");
        Document configDoc;
        try {
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(configFile);
                configDoc = parse(fis);
            } finally {
                if (fis != null)
                    fis.close();
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Error reading kauri XML configuration from " + configFile, e);
        }

        return getArtifacts(configDoc, "/*/modules/artifact", "wiring.xml");
    }

    public Set<Artifact> getClassPathArtifacts(Artifact moduleArtifact) throws MojoExecutionException {
        return getClassPathArtifacts(moduleArtifact, "KAURI-INF/classloader.xml");
    }

    public Set<Artifact> getClassPathArtifacts(Artifact moduleArtifact, String entryPath) throws MojoExecutionException {
        ZipFile zipFile = null;
        InputStream is = null;
        Document classLoaderDocument;
        try {
            zipFile = new ZipFile(moduleArtifact.getFile());
            ZipEntry zipEntry = zipFile.getEntry(entryPath);
            if (zipEntry == null) {
                log.debug("No " + entryPath + " found in " + moduleArtifact);
                return Collections.emptySet();
            } else {
                is = zipFile.getInputStream(zipEntry);
                classLoaderDocument = parse(is);
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Error reading " + entryPath + " from " + moduleArtifact, e);
        } finally {
            if (is != null)
                try { is.close(); } catch (Exception e) { /* ignore */ }
            if (zipFile != null)
                try { zipFile.close(); } catch (Exception e) { /* ignore */ }
        }

        return getArtifacts(classLoaderDocument, "/classloader/classpath/artifact", "classloader.xml from module " + moduleArtifact);
    }

    protected Set<Artifact> getArtifacts(Document configDoc, String artifactXPath, String sourceDescr) throws MojoExecutionException {
        Set<Artifact> artifacts = new HashSet<Artifact>();
        NodeList nodeList;
        try {
            nodeList = (NodeList)xpathFactory.newXPath().evaluate(artifactXPath, configDoc, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            throw new MojoExecutionException("Error resolving XPath expression " + artifactXPath + " on " + sourceDescr);
        }
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element el = (Element)nodeList.item(i);
            String groupId = el.getAttribute("groupId");
            String artifactId = el.getAttribute("artifactId");
            String version = el.getAttribute("version");
            String classifier = el.getAttribute("classifier");
            if (version.equals("") && groupId.startsWith("org.kauriproject"))
                version = kauriVersion;
            if (classifier.equals(""))
                classifier = null;

            Artifact artifact = artifactFactory.createArtifactWithClassifier(groupId, artifactId, version, "jar", classifier);

            if (filter == null || filter.include(artifact)) {
                if (!artifacts.contains(artifact)) {
                    if (resolver != null) {
                        try {
                            resolver.resolve(artifact, remoteRepositories, localRepository);
                        } catch (Exception e) {
                            throw new MojoExecutionException("Error resolving artifact listed in " + sourceDescr + ": " + artifact, e);
                        }
                    }
                    artifacts.add(artifact);
                }
            }
        }

        return artifacts;
    }

    protected Document parse(InputStream is) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        return dbf.newDocumentBuilder().parse(is);
    }

    public static interface ArtifactFilter {
        boolean include(Artifact artifact);
    }
}
