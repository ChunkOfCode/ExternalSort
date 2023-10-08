package com.example.externalfilesort.service;

import java.io.*;
import java.util.*;

import lombok.extern.slf4j.*;
import org.springframework.stereotype.*;

/**
 * Класс для сортировки файлов
 */
@Service
@Slf4j
public class ExternalSortService {

    private static final int CHUNK_SIZE = 1000;

    /**
     * Метод внешней сортировки файла
     */
    public void executeExternalSort(
        String inputPath,
        String outputPath,
        String tempDirPath
    ) throws IOException {
        var tempDirectory = createDirectory(tempDirPath);

        var sortedChunks = createSortedChunks(inputPath, tempDirectory);
        mergeSortedFileChunks(sortedChunks, outputPath);

        deleteDirectory(tempDirectory);
        log.info("Сортировка прошла успешно");
    }

    private static File createDirectory(String directoryPath) {
        var directory = new File(directoryPath);
        if (!directory.exists()) {
            var isCreated = directory.mkdir();
            if (!isCreated) {
                log.info("Не удалось создать директорию " + directoryPath);
            }
        }
        return directory;
    }

    public List<File> createSortedChunks(String inputFile, File tempDirectory) throws IOException {
        var sortedChunkFiles = new ArrayList<File>();
        try (
            var fileReader = new FileReader(inputFile);
            var bufferedReader = new BufferedReader(fileReader)
        ) {
            var currentChunk = new HashMap<Integer, List<String>>();
            createSortedChunkFile(tempDirectory, sortedChunkFiles, bufferedReader, currentChunk);

            if (!currentChunk.isEmpty()) {
                var chunkFile = writeSortedChunkToTempFile(currentChunk, tempDirectory);
                sortedChunkFiles.add(chunkFile);
            }
        }
        return sortedChunkFiles;
    }

    private void createSortedChunkFile(
        File tempDirectory,
        List<File> sortedChunkFiles,
        BufferedReader bufferedReader,
        Map<Integer, List<String>> currentChunk
    ) throws IOException {
        String currentLine;
        while ((currentLine = bufferedReader.readLine()) != null) {
            addEntryToChunk(currentLine, currentChunk);

            if (currentChunk.size() >= CHUNK_SIZE) {
                var sortedChunkFile = writeSortedChunkToTempFile(currentChunk, tempDirectory);
                sortedChunkFiles.add(sortedChunkFile);
                currentChunk.clear();
            }
        }
    }

    private void addEntryToChunk(String line, Map<Integer, List<String>> chunk) {
        var parts = line.split("\\|");
        var key = Integer.parseInt(parts[0]);
        var value = parts[1];

        if (chunk.containsKey(key)) {
            chunk.get(key).add(value);
        } else {
            var values = new ArrayList<String>();
            values.add(value);
            chunk.put(key, values);
        }
    }

    private File writeSortedChunkToTempFile(
        Map<Integer, List<String>> chunkMap,
        File tempDir
    ) throws IOException {
        var chunkFile = File.createTempFile("chunk", ".txt", tempDir);
        try (
            var fileWriter = new FileWriter(chunkFile);
            var writer = new BufferedWriter(fileWriter)
        ) {
            var sortedChunkMap = new TreeMap<>(chunkMap);
            for (Map.Entry<Integer, List<String>> entry : sortedChunkMap.entrySet()) {
                var key = entry.getKey();
                var values = entry.getValue();
                for (String value : values) {
                    writer.write(key + "|" + value);
                    writer.newLine();
                }
            }
        }
        return chunkFile;
    }

    public void mergeSortedFileChunks(
        List<File> sortedChunks,
        String outputFilePath
    ) throws IOException {
        var chunkBufferedReaders = new ArrayList<BufferedReader>();
        try {
            var minHeapOfChunks = initializeChunkHeap(sortedChunks, chunkBufferedReaders);
            writeSortedChunksToOutputFile(minHeapOfChunks, outputFilePath);
        } finally {
            for (BufferedReader reader : chunkBufferedReaders) {
                reader.close();
            }
        }
    }

    private PriorityQueue<QueueNode> initializeChunkHeap(
        List<File> sortedChunks,
        List<BufferedReader> chunkBufferedReaders
    ) throws IOException {
        var chunkHeap = new PriorityQueue<QueueNode>();
        for (File chunk : sortedChunks) {
            var bufferedReader = new BufferedReader(new FileReader(chunk));
            chunkBufferedReaders.add(bufferedReader);
            var firstChunkRow = bufferedReader.readLine().split("\\|");
            chunkHeap.add(new QueueNode(
                Integer.parseInt(firstChunkRow[0]),
                firstChunkRow[1],
                bufferedReader));
        }
        return chunkHeap;
    }

    private void writeSortedChunksToOutputFile(
        PriorityQueue<QueueNode> chunkHeap,
        String outputFilePath
    ) throws IOException {
        try (
            var fileWriter = new FileWriter(outputFilePath);
            var bufferedWriter = new BufferedWriter(fileWriter)
        ) {
            while (!chunkHeap.isEmpty()) {
                var smallestChunkNode = chunkHeap.poll();
                bufferedWriter.write(smallestChunkNode.getKey() + "|" + smallestChunkNode.getValue());
                bufferedWriter.newLine();
                addNextChunkRowToHeap(smallestChunkNode.getReader(), chunkHeap);
            }
        }
    }

    private void addNextChunkRowToHeap(
        BufferedReader chunkReader,
        PriorityQueue<QueueNode> chunkHeap
    ) throws IOException {
        var nextChunkRow = chunkReader.readLine();
        if (nextChunkRow != null) {
            var chunkRowContent = nextChunkRow.split("\\|");
            chunkHeap.add(new QueueNode(
                Integer.parseInt(chunkRowContent[0]),
                chunkRowContent[1],
                chunkReader));
        }
    }

    public static void deleteDirectory(File directory) {
        var allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        var deletionResult = directory.delete();
        if (!deletionResult) {
            log.error("Не удалось удалить файл: " + directory.getAbsolutePath());
        }
    }

}