package github.mariogarcia.spark.common;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

public final class ResourceUtils {

    public static URI loadClasspathResource(final String classpath) throws URISyntaxException {
        return ResourceUtils.class
            .getResource(classpath)
            .toURI();
    }

    public static String getResourceURI(String resourcePath) throws URISyntaxException {
        return ResourceUtils
            .loadClasspathResource(resourcePath)
            .toString();
    }
}
