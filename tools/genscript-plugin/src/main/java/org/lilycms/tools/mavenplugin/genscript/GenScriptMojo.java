package org.lilycms.tools.mavenplugin.genscript;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.factory.ArtifactFactory;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.repository.layout.ArtifactRepositoryLayout;
import org.apache.maven.artifact.repository.layout.DefaultRepositoryLayout;
import org.apache.maven.artifact.resolver.ArtifactResolver;
import org.apache.maven.model.Dependency;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.apache.maven.settings.Settings;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//
// TODO this maven plugin was copied from Daisy CMS and needs more work.
//      This will/should be looked at when working on the binary packaging of Lily.
//

/**
 * @requiresDependencyResolution runtime
 * @goal genscript
 */
public class GenScriptMojo extends AbstractMojo {

    /**
     * @parameter
     */
    private List<Script> scripts;

    /**
     * @parameter
     */
    private List<Dependency> alternativeClasspath;

    /**
     * @parameter
     */
    private boolean includeProjectInClasspath = true;

    /**
     * @parameter
     */
    private Map<String, String> defaultCliArgs = new HashMap<String, String>();

    /**
     * @parameter
     */
    private Map<String, String> defaultJvmArgs = new HashMap<String, String>();

    /**
     * @parameter default-value="${project.build.directory}"
     */
    private File devOutputDirectory;

    /**
     * @parameter default-value="${project.build.directory}/dist-scripts"
     */
    private File distOutputDirectory;

    /**
     * @parameter default-value="${settings}"
     * @readonly
     * @required
     */
    private Settings settings;

    /**
     * @parameter expression="${project}"
     * @readonly
     * @required
     */
    private MavenProject project;

    /**
     * Maven Artifact Factory component.
     *
     * @component
     */
    protected ArtifactFactory artifactFactory;

    /**
     * Artifact Resolver component.
     *
     * @component
     */
    protected ArtifactResolver resolver;

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

    private ArtifactRepositoryLayout m2layout = new DefaultRepositoryLayout();

    enum Platform {
        UNIX("/", ":", "$", "", ""), WINDOWS("\\", ";", "%", "%", ".bat");

        Platform(String fileSeparator, String pathSeparator, String envPrefix, String envSuffix, String extension) {
            this.fileSeparator = fileSeparator;
            this.pathSeparator = pathSeparator;
            this.envPrefix = envPrefix;
            this.envSuffix = envSuffix;
            this.extension = extension;
        }

        private String fileSeparator;
        private String pathSeparator;
        private String envPrefix;
        private String envSuffix;
        private String extension;
    }

    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            for (Script script: scripts) {
                generateScripts(script);
            }
        } catch (IOException ioe) {
            throw new MojoFailureException("Failed to generate script ", ioe);
        }
    }

    private void generateScripts(Script script) throws IOException, MojoExecutionException {
        devOutputDirectory.mkdirs();
        distOutputDirectory.mkdirs();

        File win = new File(distOutputDirectory, script.getBasename().concat(".bat"));
        File winDev = new File(devOutputDirectory, script.getBasename().concat(".bat"));
        File unix = new File(distOutputDirectory, script.getBasename());
        File unixDev = new File(devOutputDirectory, script.getBasename());

        for (Platform platform : Platform.values()) {
            String cp = generateClassPath(false, platform);
            String dev_cp = generateClassPath(true, platform);

            File dist = new File(distOutputDirectory, script.getBasename().concat(platform.extension));
            File dev = new File(devOutputDirectory, script.getBasename().concat(platform.extension));

            generateScript(dist, platform.name().toLowerCase() + ".template", script.getMainClass(), cp, platform);
            generateScript(dev, platform.name().toLowerCase() + "-dev.template", script.getMainClass(), dev_cp, platform);
        }

        if (new File("/bin/chmod").exists()) {
            Runtime.getRuntime().exec("/bin/chmod a+x " + unix.getAbsolutePath());
            Runtime.getRuntime().exec("/bin/chmod a+x " + unixDev.getAbsolutePath());
        }
    }

    private void generateScript(File outputFile, String template, String mainClass,
            String classPath, Platform platform) throws IOException {

        InputStream is = getClass().getResourceAsStream("/org/lilycms/tools/mavenplugin/genscript/".concat(template));
        String result = streamToString(is);


        String defaultCliArgs = this.defaultCliArgs.get(platform.toString().toLowerCase());
        if (defaultCliArgs == null)
            defaultCliArgs = "";

        String defaultJvmArgs = this.defaultJvmArgs.get(platform.toString().toLowerCase());
        if (defaultJvmArgs == null)
            defaultJvmArgs = "";

        String separator = "$$$";
        result = result.replaceAll(Pattern.quote(separator.concat("CLASSPATH").concat(separator)), Matcher.quoteReplacement(classPath)).
            replaceAll(Pattern.quote(separator.concat("MAINCLASS").concat(separator)), Matcher.quoteReplacement(mainClass)).
            replaceAll(Pattern.quote(separator.concat("DEFAULT_CLI_ARGS").concat(separator)), Matcher.quoteReplacement(defaultCliArgs)).
            replaceAll(Pattern.quote(separator.concat("DEFAULT_JVM_ARGS").concat(separator)), Matcher.quoteReplacement(defaultJvmArgs));

        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        writer.write(result);
        writer.close();
    }

    private String streamToString(InputStream in) throws IOException {
        StringBuffer out = new StringBuffer();
        byte[] b = new byte[4096];
        for (int n; (n = in.read(b)) != -1;) {
            out.append(new String(b, 0, n));
        }
        return out.toString();
    }

    private String generateClassPath(boolean isDevelopment, Platform platform) throws MojoExecutionException {
        StringBuilder result = new StringBuilder();
        ArtifactRepositoryLayout layout = m2layout;
        String basePath = isDevelopment ? settings.getLocalRepository() : platform.envPrefix.concat("LILY_HOME").concat(platform.envSuffix).concat(platform.fileSeparator).concat("lib");

        for (Artifact artifact: getClassPath()) {
            result.append(basePath).append(platform.fileSeparator).append(layout.pathOf(artifact));
            result.append(platform.pathSeparator);
        }

        if (includeProjectInClasspath) {
            if (isDevelopment) {
                result.append(project.getBuild().getOutputDirectory());
            } else {
                result.append(basePath).append(platform.fileSeparator).append(layout.pathOf(project.getArtifact()));
            }
            result.append(platform.pathSeparator);
        }

        result.append(platform.envPrefix).append("LILY_CLI_CLASSPATH").append(platform.envSuffix);


        return result.toString();
    }

    private List<Artifact> getClassPath() throws MojoExecutionException {
        if (alternativeClasspath != null && alternativeClasspath.size() > 0) {
            return getAlternateClassPath();
        } else {
            return (List<Artifact>)project.getRuntimeArtifacts();
        }
    }

    private List<Artifact> getAlternateClassPath() throws MojoExecutionException {
        List<Artifact> result = new ArrayList<Artifact>();
        for (Dependency dependency : alternativeClasspath) {
            Artifact artifact = artifactFactory.createArtifactWithClassifier(dependency.getGroupId(),
                    dependency.getArtifactId(), dependency.getVersion(), "jar", dependency.getClassifier());
            try {
                resolver.resolve(artifact, remoteRepositories, localRepository);
            } catch (Exception e) {
                throw new MojoExecutionException("Error resolving artifact: " + artifact, e);
            }
            result.add(artifact);
        }
        return result;
    }

}
