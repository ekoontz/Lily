package org.lilycms.tools.plugin.kauridepresolver;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.factory.ArtifactFactory;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.resolver.ArtifactResolver;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.kauriproject.runtime.model.SourceLocations;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 *
 * @goal resolve
 * @requiresDependencyResolution resolve
 * @description Resolve (download) all the dependencies of a Kauri project starting from wiring.xml.
 */
public class KauriDependencyResolver extends AbstractMojo {
    /**
     * Location of the conf directory.
     *
     * @parameter expression="${basedir}/conf"
     * @required
     */
    protected String confDirectory;

    /**
     * Kauri version.
     *
     * @parameter
     * @required
     */
    protected String kauriVersion;

    /**
     * Location of the module-source-locations.properties file.
     *
     * @parameter
     */
    protected String moduleSourceLocations;

    /**
     * @parameter expression="${project.groupId}"
     * @required
     * @readonly
     */
    private String projectGroupId;

    /**
     * @parameter expression="${project.artifactId}"
     * @required
     * @readonly
     */
    private String projectArtifactId;

    /**
     * @parameter expression="${project.version}"
     * @required
     * @readonly
     */
    private String projectVersion;

    /**
     * Maven Artifact Factory component.
     *
     * @component
     */
    protected ArtifactFactory artifactFactory;

    /**
     * Remote repositories used for the project.
     *
     * @parameter expression="${project.remoteArtifactRepositories}"
     * @required
     * @readonly
     */
    protected List remoteRepositories;

    /**
     * Local Repository.
     *
     * @parameter expression="${localRepository}"
     * @required
     * @readonly
     */
    protected ArtifactRepository localRepository;

    /**
     * Artifact Resolver component.
     *
     * @component
     */
    protected ArtifactResolver resolver;

    protected XPathFactory xpathFactory = XPathFactory.newInstance();
    protected SourceLocations sourceLocations = new SourceLocations();

    public void execute() throws MojoExecutionException, MojoFailureException {
        if (moduleSourceLocations != null) {
            File sourceLocationsFile = new File(moduleSourceLocations);
            FileInputStream sourceLocationsStream = null;
            try {
                sourceLocationsStream = new FileInputStream(sourceLocationsFile);
                sourceLocations = new SourceLocations(sourceLocationsStream, sourceLocationsFile.getParentFile().getAbsolutePath());
            } catch (Exception e) {
                throw new MojoExecutionException("Problem reading module source locations file from " +
                        sourceLocationsFile.getAbsolutePath(), e);
            } finally {
                if (sourceLocationsStream != null) {
                    try { sourceLocationsStream.close(); } catch (IOException e) { e.printStackTrace(); }
                }
            }
        }

        Set<Artifact> moduleArtifacts = getModuleArtifactsFromKauriConfig();

        for (Artifact moduleArtifact : moduleArtifacts) {
            getClassPathArtifacts(moduleArtifact);
        }
    }

    protected Set<Artifact> getModuleArtifactsFromKauriConfig() throws MojoExecutionException {
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

    protected Set<Artifact> getClassPathArtifacts(Artifact moduleArtifact) throws MojoExecutionException {
        String entryPath = "KAURI-INF/classloader.xml";
        ZipFile zipFile = null;
        InputStream is = null;
        Document classLoaderDocument;
        try {
            zipFile = new ZipFile(moduleArtifact.getFile());
            ZipEntry zipEntry = zipFile.getEntry(entryPath);
            if (zipEntry == null) {
                getLog().debug("No " + entryPath + " found in " + moduleArtifact);
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

            if (artifact.getGroupId().equals(projectGroupId) &&
                    artifact.getArtifactId().equals(projectArtifactId) &&
                    artifact.getVersion().equals(projectVersion)) {
                // Current project's artifact is not yet deployed, therefore do not treat it
            } else if (sourceLocations.getSourceLocation(artifact.getGroupId(), artifact.getArtifactId()) != null) {
                // It is one of the artifacts of this project, hence the dependencies will have been
                // downloaded by Maven. Skip it.
            } else {
                if (!artifacts.contains(artifact)) {
                    try {
                        resolver.resolve(artifact, remoteRepositories, localRepository);
                    } catch (Exception e) {
                        throw new MojoExecutionException("Error resolving artifact listed in " + sourceDescr + ": " + artifact, e);
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

}
