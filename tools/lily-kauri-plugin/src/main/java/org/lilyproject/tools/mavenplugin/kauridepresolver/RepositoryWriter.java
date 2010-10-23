package org.lilyproject.tools.mavenplugin.kauridepresolver;

import org.apache.commons.io.FileUtils;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.repository.layout.ArtifactRepositoryLayout;
import org.apache.maven.artifact.repository.layout.DefaultRepositoryLayout;
import org.apache.maven.plugin.MojoExecutionException;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public class RepositoryWriter {

    public static void write(Set<Artifact> artifacts, String targetDirectory) throws MojoExecutionException {
        ArtifactRepositoryLayout m2layout = new DefaultRepositoryLayout();

        for (Artifact artifact : artifacts) {
            File src = artifact.getFile();
            File dest = new File(targetDirectory, m2layout.pathOf(artifact));
            try {
                FileUtils.copyFile(src, dest);
            } catch (IOException e) {
                throw new MojoExecutionException("Error copying file " + src + " to " + dest);
            }

            // Kauri does not need the pom files, but let's copy them anyway, for informational purposes
            File srcPom = pomFile(src);
            File destPom = pomFile(dest);
            if (srcPom != null && srcPom.exists()) {
                try {
                    FileUtils.copyFile(srcPom, destPom);
                } catch (IOException e) {
                    throw new MojoExecutionException("Error copying file " + srcPom + " to " + destPom);
                }
            }
        }
    }

    public static File pomFile(File jarFile) throws MojoExecutionException {
        String path = jarFile.getAbsolutePath();

        if (!path.endsWith(".jar")) {
            return null;
        }

        return new File(path.replaceAll("\\.jar$", ".pom"));
    }
}
