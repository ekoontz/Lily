package org.lilycms.tools.mavenplugin.genscript;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.repository.layout.ArtifactRepositoryLayout;
import org.apache.maven.artifact.repository.layout.DefaultRepositoryLayout;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.apache.maven.settings.Settings;

import java.io.*;
import java.util.List;
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
     * @parameter default-value="${project.build.directory}"
     */
    private File outputDirectory;

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

    private ArtifactRepositoryLayout m2layout = new DefaultRepositoryLayout();

    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            for (Script script: scripts) {
                generateScripts(script);
            }
        } catch (IOException ioe) {
            throw new MojoFailureException("Failed to generate script ", ioe);
        }
    }

    private void generateScripts(Script script) throws IOException {
        outputDirectory.mkdirs();

//        File win = new File(outputDirectory, script.getBasename().concat(".bat"));
        File winDev = new File(outputDirectory, script.getBasename().concat(".bat"));
//        File unix = new File(outputDirectory, script.getBasename());
        File unixDev = new File(outputDirectory, script.getBasename());

        String win_cp = generateClassPath(false, "\\", ";", "%", "%");
        String win_dev_cp = generateClassPath(true, "/", ";", "%", "%");
        String unix_cp = generateClassPath(false, "/", ":", "$", "");
        String unix_dev_cp = generateClassPath(true, "/", ":", "$", "");

//        generateScript(win, "windows.template", script.getMainClass(), win_cp);
        generateScript(winDev, "windows-dev.template", script.getMainClass(), win_dev_cp);
//        generateScript(unix, "unix.template", script.getMainClass(), unix_cp);
        generateScript(unixDev, "unix-dev.template", script.getMainClass(), unix_dev_cp);

        if (new File("/bin/chmod").exists()) {
//            Runtime.getRuntime().exec("/bin/chmod a+x " + unix.getAbsolutePath());
            Runtime.getRuntime().exec("/bin/chmod a+x " + unixDev.getAbsolutePath());
        }

    }

    private void generateScript(File outputFile, String template, String mainClass,
            String classPath) throws IOException {

        InputStream is = getClass().getResourceAsStream("/org/lilycms/tools/mavenplugin/genscript/".concat(template));
        String result = streamToString(is);

        String separator = "$$$";
        result = result.replaceAll(Pattern.quote(separator.concat("CLASSPATH").concat(separator)), Matcher.quoteReplacement(classPath))
            .replaceAll(Pattern.quote(separator.concat("MAINCLASS").concat(separator)), Matcher.quoteReplacement(mainClass));

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

    private String generateClassPath(boolean isDevelopment, String fileSeparator, String pathSeparator, String envPrefix, String envSuffix) {
        StringBuilder result = new StringBuilder();
        ArtifactRepositoryLayout layout = m2layout;
        String basePath = isDevelopment ? settings.getLocalRepository() : envPrefix.concat("DAISY_HOME").concat(envSuffix).concat(fileSeparator).concat("lib");

        for (Artifact artifact: (List<Artifact>)project.getRuntimeArtifacts()) {
            result.append(basePath).append(fileSeparator).append(layout.pathOf(artifact));
            result.append(pathSeparator);
        }
        if (isDevelopment) {
            result.append(project.getBuild().getOutputDirectory());
        } else {
            result.append(basePath).append(fileSeparator).append(layout.pathOf(project.getArtifact()));
        }
        result.append(pathSeparator);

        result.append(envPrefix).append("LILY_CLI_CLASSPATH").append(envSuffix);


        return result.toString();
    }

}
