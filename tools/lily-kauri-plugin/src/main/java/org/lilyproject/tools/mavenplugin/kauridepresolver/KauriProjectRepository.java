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
 * @goal assemble-project-repository
 * @requiresDependencyResolution runtime
 * @description Creates a Maven-style repository for Kauri with all the dependencies of a Kauri project,
 *              based upon reading the wiring.xml.
 */
public class KauriProjectRepository extends AbstractMojo {
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
     * Location of the conf directory.
     *
     * @parameter expression="${basedir}/target/kauri-repository"
     * @required
     */
    protected String targetDirectory;

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
        KauriProjectClasspath cp = new KauriProjectClasspath(confDirectory, kauriVersion, getLog(), null,
                artifactFactory, resolver, remoteRepositories, localRepository);

        Set<Artifact> artifacts = cp.getAllArtifacts();
        RepositoryWriter.write(artifacts, targetDirectory);
    }

}
