package org.lilyproject.tools.mavenplugin.kauridepresolver;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.factory.ArtifactFactory;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.resolver.ArtifactResolver;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

import java.util.List;
import java.util.Set;

/**
 *
 * @goal resolve-runtime-dependencies
 * @requiresDependencyResolution runtime
 * @description Resolve (download) all the dependencies to run the Kauri Runtime.
 */
public class KauriRuntimeDependencyResolver extends AbstractMojo {
    /**
     * Kauri version.
     *
     * @parameter
     * @required
     */
    protected String kauriVersion;

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

    public void execute() throws MojoExecutionException, MojoFailureException {
        KauriProjectClasspath cp = new KauriProjectClasspath(null, kauriVersion, getLog(), null,
                artifactFactory, resolver, remoteRepositories, localRepository);

        Artifact runtimeLauncherArtifact = artifactFactory.createArtifact("org.kauriproject", "kauri-runtime-launcher", kauriVersion, "runtime", "jar");

        try {
            resolver.resolve(runtimeLauncherArtifact, remoteRepositories, localRepository);
        } catch (Exception e) {
            throw new MojoExecutionException("Error resolving artifact: " + runtimeLauncherArtifact, e);
        }

        Set<Artifact> artifacts = cp.getClassPathArtifacts(runtimeLauncherArtifact, "org/kauriproject/launcher/classloader.xml");
        artifacts.add(runtimeLauncherArtifact);
    }
}
