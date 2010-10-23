package org.lilyproject.tools.mavenplugin.hbaseexclusions;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import java.util.*;

/**
 * This is a temporary Maven plugin used in the lily-hbase-client project to check that there are no
 * redundant dependencies. Otherwise prints out the necessary exclusion-statements and fails the build.
 *
 * <p>This will be invalidated once there is a solution for this issue:
 * https://issues.apache.org/jira/browse/HBASE-2170
 *
 * @requiresDependencyResolution runtime
 * @goal generate-exclusions
 */
public class HBaseExclusionsMojo  extends AbstractMojo {
    /**
     * @parameter expression="${project}"
     * @readonly
     * @required
     */
    private MavenProject project;

    public void execute() throws MojoExecutionException, MojoFailureException {
        List<Artifact> dependencies = (List<Artifact>)project.getRuntimeArtifacts();
        List<Artifact> excludes = new ArrayList<Artifact>(dependencies.size());

        for (Artifact artifact : dependencies) {
            // Rather than simply outputting all dependencies as excludes, we only want to output
            // an exclude for the direct children of the allowed artifact, because the other ones
            // will be disabled recursively by Maven.
            int allowedParentPos = getAllowedParentPostion(artifact.getDependencyTrail());
            if (allowedParentPos != -1 && allowedParentPos == artifact.getDependencyTrail().size() - 2) {
                excludes.add(artifact);
            }
        }

        if (excludes.size() > 0) {
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println("Please add these excludes to the lily-hbase-client pom:");
            System.out.println();
            for (Artifact artifact : excludes) {
                System.out.println("<exclusion>");
                System.out.println("  <groupId>" + artifact.getGroupId() + "</groupId>");
                System.out.println("  <artifactId>" + artifact.getArtifactId() + "</artifactId>");
                System.out.println("</exclusion>");
            }
            System.out.println();
            System.out.println();
            System.out.println();

            throw new MojoExecutionException("lily-hbase-client is missing some excludes, please adjust");
        }
    }

    private int getAllowedParentPostion(List<String> trail) {
        for (int i = trail.size() - 1; i >= 0; i--) {
            String artifact = trail.get(i);

            int groupIdEnd = artifact.indexOf(':');
            String groupId = artifact.substring(0, groupIdEnd);
            int artifactIdEnd = artifact.indexOf(':', groupIdEnd + 1);
            String artifactId = artifact.substring(groupIdEnd + 1, artifactIdEnd);

            if (isAllowed(groupId, artifactId)) {
                return i;
            }
        }
        return -1;
    }

    private static Set<String> ALLOWED_ARTIFACTS = new HashSet<String>();
    static {
        ALLOWED_ARTIFACTS.add("org.apache.hbase:hbase");
        ALLOWED_ARTIFACTS.add("org.apache.hadoop:zookeeper");
        ALLOWED_ARTIFACTS.add("org.apache.hadoop:hadoop-core");
        ALLOWED_ARTIFACTS.add("com.google.guava:guava");
    }
    
    private boolean isAllowed(String groupId, String artifactId) {
        return ALLOWED_ARTIFACTS.contains(groupId + ":" + artifactId);
    }
}
