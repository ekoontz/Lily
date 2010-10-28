/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.tools.mavenplugin.genscript;

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
    private List<Parameter> defaultCliArgs;

    /**
     * @parameter
     */
    private List<Parameter> defaultJvmArgs;

    /**
     * @parameter
     */
    private List<Parameter> classPathPrefix;

    /**
     * @parameter
     */
    private List<Parameter> beforeJavaHook;

    /**
     * @parameter default-value="${project.build.directory}"
     */
    private File devOutputDirectory;

    /**
     * @parameter default-value="${project.build.directory}/dist-scripts"
     */
    private File distOutputDirectory;

    private Map<Mode, File> outputDirectories = new HashMap<Mode, File>();

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
    
    enum Mode {
        DEV("-dev"),
        DIST("");

        Mode(String templateSuffix) {
            this.templateSuffix = templateSuffix;
        }

        private String templateSuffix;
    }

    public void execute() throws MojoExecutionException, MojoFailureException {
        outputDirectories.put(Mode.DEV, devOutputDirectory);
        outputDirectories.put(Mode.DIST, distOutputDirectory);

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

        for (Mode mode : Mode.values()) {
            for (Platform platform : Platform.values()) {
                String cp = generateClassPath(mode == Mode.DEV, platform);

                File scriptDir = outputDirectories.get(mode);
                scriptDir.mkdirs();

                File scriptFile = new File(scriptDir, script.getBasename().concat(platform.extension));

                generateScript(scriptFile, platform.name().toLowerCase() + mode.templateSuffix + ".template",
                        script.getMainClass(), cp, platform, mode);

                if (new File("/bin/chmod").exists()) {
                    Runtime.getRuntime().exec("/bin/chmod a+x " + scriptFile.getAbsolutePath());
                }
            }
        }
    }

    private void generateScript(File outputFile, String template, String mainClass,
            String classPath, Platform platform, Mode mode) throws IOException {

        InputStream is = getClass().getResourceAsStream("/org/lilyproject/tools/mavenplugin/genscript/".concat(template));
        String result = streamToString(is);


        String defaultCliArgs = getParameter(this.defaultCliArgs, platform, mode, "");

        String defaultJvmArgs = getParameter(this.defaultJvmArgs, platform, mode, "");

        String beforeJavaHook = getParameter(this.beforeJavaHook, platform, mode, "");

        String classPathPrefix = getParameter(this.classPathPrefix, platform, mode, "");

        String separator = "$$$";
        result = result.replaceAll(Pattern.quote(separator.concat("CLASSPATH").concat(separator)), Matcher.quoteReplacement(classPath)).
            replaceAll(Pattern.quote(separator.concat("CLASSPATH_PREFIX").concat(separator)), Matcher.quoteReplacement(classPathPrefix)).
            replaceAll(Pattern.quote(separator.concat("MAINCLASS").concat(separator)), Matcher.quoteReplacement(mainClass)).
            replaceAll(Pattern.quote(separator.concat("DEFAULT_CLI_ARGS").concat(separator)), Matcher.quoteReplacement(defaultCliArgs)).
            replaceAll(Pattern.quote(separator.concat("DEFAULT_JVM_ARGS").concat(separator)), Matcher.quoteReplacement(defaultJvmArgs)).
            replaceAll(Pattern.quote(separator.concat("BEFORE_JAVA_HOOK").concat(separator)), Matcher.quoteReplacement(beforeJavaHook));

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
            result.append(basePath).append(platform.fileSeparator).append(artifactPath(artifact, platform));
            result.append(platform.pathSeparator);
        }

        if (includeProjectInClasspath) {
            if (isDevelopment) {
                result.append(project.getBuild().getOutputDirectory());
            } else {
                result.append(basePath).append(platform.fileSeparator).append(artifactPath(project.getArtifact(), platform));
            }
            result.append(platform.pathSeparator);
        }

        return result.toString();
    }

    private String artifactPath(Artifact artifact, Platform platform) {
        // pathOf always creates a path with slashes, irrespective of the current platform
        String artifactPath = m2layout.pathOf(artifact);
        artifactPath = artifactPath.replaceAll("/", Matcher.quoteReplacement(platform.fileSeparator));
        return artifactPath;
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

    private String getParameter(List<Parameter> parameters, Platform platform, Mode mode, String defaultValue) {
        if (parameters == null)
            return defaultValue;

        for (Parameter parameter : parameters) {
            if (parameter.platform.toUpperCase().equals(platform.name()) && parameter.mode.toUpperCase().equals(mode.name())) {
                return parameter.value;
            }
        }
        return defaultValue;
    }

}
