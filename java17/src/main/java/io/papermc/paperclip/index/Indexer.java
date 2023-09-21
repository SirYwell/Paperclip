package io.papermc.paperclip.index;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Indexer {
    private static final Path INDEX_FILE = Path.of("class_index.dat");

    public static Index createIndex(byte[] hash, URL[] urls) {
        if (Files.exists(INDEX_FILE)) {
            try {
                return loadFromFile(hash);
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Failed to load index from file: " + e.getMessage());
                // fall through, create new index
            }
        }
        return createFreshIndex(hash, urls);
    }

    private static Index createFreshIndex(byte[] hash, URL[] urls) {
        Index index = scan(urls);
        writeToFile(hash, index);
        return index;
    }

    private static void writeToFile(byte[] hash, Index index) {
        try (OutputStream rawOutputStream = Files.newOutputStream(INDEX_FILE);
                OutputStream bos = new BufferedOutputStream(rawOutputStream);
                DataOutputStream dataOut = new DataOutputStream(bos)) {
            dataOut.write(hash);
            writeIndex(dataOut, index);
        } catch (IOException e) {
            System.err.println("Failed to write index:");
            e.printStackTrace(System.err);
        }
    }

    private static void writeIndex(DataOutput dataOut, Index data) throws IOException {
        List<String> uniqueStrings = Stream.concat(combineMap(data.classLookup()), combineMap(data.resourceLookup()))
                .distinct().toList();
        Map<String, Integer> stringIndices = IntStream.range(0, uniqueStrings.size()).boxed().collect(Collectors.toMap(uniqueStrings::get, it -> it));

        dataOut.writeInt(uniqueStrings.size());
        for (String s : uniqueStrings) {
            dataOut.writeUTF(s);
        }
        Map<String, List<String>> classLookup = data.classLookup();

        writeMap(dataOut, classLookup, stringIndices);
        writeMap(dataOut, data.resourceLookup(), stringIndices);
    }

    private static Stream<String> combineMap(Map<String, List<String>> map) {
        return Stream.concat(map.values().stream().flatMap(Collection::stream), map.keySet().stream());
    }

    private static void writeMap(DataOutput dataOut, Map<String, List<String>> lookup, Map<String, Integer> stringIndices) throws IOException {
        dataOut.writeInt(lookup.size());
        for (Map.Entry<String, List<String>> entry : lookup.entrySet()) {
            dataOut.writeInt(stringIndices.get(entry.getKey()));
            dataOut.writeInt(entry.getValue().size());
            for (String s : entry.getValue()) {
                dataOut.writeInt(stringIndices.get(s));
            }
        }
    }

    private static Index readIndex(DataInput dataIn) throws IOException {
        int stringCount = dataIn.readInt();
        String[] stringEntries = new String[stringCount];
        for (int i = 0; i < stringCount; i++) {
            stringEntries[i] = dataIn.readUTF();
        }

        Map<String, List<String>> classLookup = readMap(dataIn, stringEntries);
        Map<String, List<String>> resourceLookup = readMap(dataIn, stringEntries);

        return new Index(classLookup, resourceLookup);
    }

    private static Map<String, List<String>> readMap(DataInput dataIn, String[] stringEntries) throws IOException {
        int entryCount = dataIn.readInt();
        Map<String, List<String>> result = new HashMap<>(calculateHashMapCapacity(entryCount));

        for (int i = 0; i < entryCount; i++) {
            String key = stringEntries[dataIn.readInt()];
            int valueSize = dataIn.readInt();
            List<String> value = new ArrayList<>(valueSize);
            for (int j = 0; j < valueSize; j++) {
                value.add(stringEntries[dataIn.readInt()]);
            }
            result.put(key, value);
        }
        return result;
    }

    private static Index loadFromFile(byte[] hash) throws IOException, ClassNotFoundException {
        try (InputStream rawInputStream = Files.newInputStream(INDEX_FILE);
                BufferedInputStream bis = new BufferedInputStream(rawInputStream);
                DataInputStream dataIn = new DataInputStream(bis)
                ) {
            byte[] storedHash = new byte[hash.length];
            dataIn.readFully(storedHash);
            if (!Arrays.equals(hash, storedHash)) {
                throw new IOException("Outdated file");
            }
            return readIndex(dataIn);
        }
    }

    static int calculateHashMapCapacity(int numMappings) {
        return (int) Math.ceil(numMappings / (double) 0.75f);
    }

    private static Index scan(URL[] urls) {
        Map<String, List<String>> classIndex = new HashMap<>();
        Map<String, List<String>> resourceIndex = new HashMap<>();
        for (URL url : urls) {
            checkUrl(url);
            String sourceJar = url.getPath();
            for (String resource : scan(url)) {
                if (resource.endsWith(".class")) {
                    classIndex.computeIfAbsent(resource, __ -> new ArrayList<>(1)).add(sourceJar);
                } else {
                    resourceIndex.computeIfAbsent(resource, __ -> new ArrayList<>(1)).add(sourceJar);
                }

            }
        }
        return new Index(classIndex, resourceIndex);
    }

    private static void checkUrl(URL url) {
        if (!url.getProtocol().equals("file")) {
            throw new IllegalStateException("Unsupported protocol: " + url.getProtocol() + ". Must be 'file'");
        }
    }

    private static List<String> scan(URL url) {
        try (FileSystem fileSystem = FileSystems.newFileSystem(Path.of(url.toURI()))) {
            List<String> allFiles = new ArrayList<>();
            for (Path rootDirectory : fileSystem.getRootDirectories()) {
                try (Stream<Path> pathStream = Files.find(rootDirectory, Integer.MAX_VALUE,
                        ((path, basicFileAttributes) -> basicFileAttributes.isRegularFile()))) {
                    List<String> found = pathStream.map(Path::toString).map(s -> s.substring(1)) // remove leading '/'
                            .toList();
                    allFiles.addAll(found);
                }
            }
            return allFiles;
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
