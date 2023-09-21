package io.papermc.paperclip.index;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

public class IndexedClassLoader extends ClassLoader {
    private final Index index;
    private final Map<String, Loader> jarLookup;

    static {
        registerAsParallelCapable();
    }

    public IndexedClassLoader(String name, Index index, URL[] urls, ClassLoader parent) {
        super("IndexedClassLoader-" + name, parent);
        this.index = index;
        this.jarLookup = Arrays.stream(urls).collect(Collectors.toMap(URL::getPath, url -> {
            try {
                return new Loader(url);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        String path = binaryNameToPath(name);
        String targetJar = firstElement(index.classLookup(), path);
        if (targetJar == null) {
            throw new ClassNotFoundException("No target jar associated with class " + name);
        }
        Loader loader = jarLookup.get(targetJar);
        byte[] byteCode = loadFromJar(loader.getJarFile(), path);
        return defineClass(name, byteCode, 0, byteCode.length);
    }

    private String firstElement(Map<String, List<String>> lookup, String path) {
        List<String> strings = lookup.get(path);
        if (strings == null) {
            return null;
        }
        return strings.get(0);
    }

    @Override
    protected URL findResource(String name) {
        String targetJar = firstElement(index.resourceLookup(), name);
        if (targetJar == null) {
            if ((targetJar = firstElement(index.classLookup(), name)) == null) {
                return null;
            }
        }
        Loader loader = jarLookup.get(targetJar);
        return loader.getResource(name);
    }

    @Override
    protected Enumeration<URL> findResources(String name) {
        List<URL> result = new ArrayList<>();
        for (String targetJar : index.resourceLookup().getOrDefault(name, List.of())) {
            Loader loader = jarLookup.get(targetJar);
            URL resource = loader.getResource(name);
            if (resource != null) {
                result.add(resource);
            }
        }
        for (String targetJar : index.classLookup().getOrDefault(name, List.of())) {
            Loader loader = jarLookup.get(targetJar);
            URL resource = loader.getResource(name);
            if (resource != null) {
                result.add(resource);
            }
        }
        return Collections.enumeration(result);
    }

    private static String binaryNameToPath(String binaryName) {
        return binaryName.replace('.', '/') + ".class";
    }

    private byte[] loadFromJar(JarFile jarFile, String name) throws ClassNotFoundException {
        JarEntry jarEntry = jarFile.getJarEntry(name);
        try (InputStream inputStream = jarFile.getInputStream(jarEntry)) {
            return inputStream.readAllBytes();
        } catch (IOException e) {
            throw new ClassNotFoundException("Exception while loading class " + name, e);
        }
    }

    private static final class Loader {
        // TODO we can probably use less strict access modes
        private static final VarHandle JAR_FILE_HANDLE;

        static {
            try {
                JAR_FILE_HANDLE = MethodHandles.lookup().findVarHandle(Loader.class, "jarFile", JarFile.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }

        private final URL jarUrl; // 'jar' protocol
        private final String filePath;
        @SuppressWarnings("unused") // actually used
        private volatile JarFile jarFile;

        private Loader(URL fileUrl) throws MalformedURLException {
            this.jarUrl = new URL("jar", "", -1, fileUrl + "!/");
            this.filePath = fileUrl.getFile();
        }

        URL getResource(String name) {
            try {
                return new URL(jarUrl, name); // must exist as we found it in our index
            } catch (MalformedURLException e) {
                return null; // not allowed to throw exception
            }
        }

        JarFile getJarFile() {
            JarFile jarFile = this.jarFile;
            if (jarFile != null) {
                return jarFile;
            }
            return loadJarFile();
        }

        private JarFile loadJarFile() {
            try {
                JarFile jarFile = new JarFile(new File(filePath), true, ZipFile.OPEN_READ, Runtime.version()); // use the url to the file
                JarFile result = (JarFile) JAR_FILE_HANDLE.compareAndExchange(this, null, jarFile);
                if (result != null) {
                    jarFile.close(); // not successful, close again
                    return result; // return the winner
                }
                return jarFile; // successful
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

}
