package com.alibaba.lindorm.contest.engine;

import com.alibaba.lindorm.contest.TSDBEngine;
import com.alibaba.lindorm.contest.data.CarDataFileModel;
import com.alibaba.lindorm.contest.index.LastIndexFileModel;
import com.alibaba.lindorm.contest.structs.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NioTSDBEngineImpl extends TSDBEngine {

    private static final int NUM_FOLDERS =300;
    private static final ConcurrentMap< Vin , Lock> VIN_LOCKS = new ConcurrentHashMap<>();
    private static ConcurrentMap < Vin, CarDataFileModel> OUT_FILES = new ConcurrentHashMap<>();

    private boolean connected = false;
    public static int columnsNum;
    public static ArrayList<String> columnsName = new ArrayList<>();
    public static ArrayList<ColumnValue.ColumnType > columnsType = new ArrayList<>();
    public AtomicBoolean initLastIndexed = new AtomicBoolean (false);
    private LastIndexFileModel lastIndexFileModel;

    public NioTSDBEngineImpl(File dataPath) {
        super(dataPath);
    }

    @Override
    public void connect() throws IOException {
        LocalDateTime startTime = LocalDateTime . now ();
        System.out.println (" connect start :"+ startTime);
        if(connected){
            throw  new IOException("Connected");
        }
        File schemaFile = new File(dataPath, "schema.txt");
        if(!schemaFile.exists() || !schemaFile.isFile()){
            this.lastIndexFileModel = new LastIndexFileModel(dataPath, columnsName, columnsNum);
            connected = true;
            return;
        }

        try(BufferedReader reader = new BufferedReader(new FileReader(schemaFile))){
            String line;
            if((line = reader.readLine()) != null && !line.isEmpty()){
                String[] parts = line.split(",");
                columnsNum = Integer.parseInt(parts[0]);
                if(columnsNum <= 0){
                    System.err.println("Unexpected column nums : [ " + columnsNum + "]");
                    throw new RuntimeException();
                }
                int index = 1;
                for(int i = 0; i < columnsNum; i ++){
                    columnsName.add(parts[index ++]);
                    columnsType.add(ColumnValue.ColumnType.valueOf(parts[index ++]));
                }
            }
        }

        // load lastIndex
        this.lastIndexFileModel = new LastIndexFileModel(dataPath, columnsName, columnsNum);
        lastIndexFileModel.initLastIndexRowMap(initLastIndexed);
        connected = true;
    }

    @Override
    public void createTable(String tableName, Schema schema) throws IOException {
        LocalDateTime startTime = LocalDateTime.now();
        System.out.println("createTable start : " + startTime);

        Map<String, ColumnValue.ColumnType> columnTypeMap = schema.getColumnTypeMap();
        columnsNum = columnTypeMap.size();
        columnsName = new ArrayList<>();
        columnsType = new ArrayList<>();

        for(Map.Entry<String, ColumnValue.ColumnType> entry : columnTypeMap.entrySet()){
            columnsName.add(entry.getKey());
            columnsType.add(entry.getValue());
        }
        this.lastIndexFileModel = new LastIndexFileModel(dataPath, columnsName, columnsNum);
    }

    @Override
    public void shutdown() {
        LocalDateTime startTime = LocalDateTime.now();
        System.out.println("shutdown start : " + startTime);

        if(!connected){
            return;
        }

        // persist last index
        initLastIndexed.set(false);
        lastIndexFileModel.flushLastIndexRowMap();

        for(CarDataFileModel fout : OUT_FILES.values()){
            try {
                fout.close();
            } catch (IOException e){
                e.printStackTrace();
            }
        }

        OUT_FILES.clear();
        VIN_LOCKS.clear();

        // persist the schema
        try {
            File schemaFile = new File(getDataPath(), "schema.txt");
            schemaFile.delete();
            schemaFile.createNewFile();
            BufferedWriter writer = new BufferedWriter(new FileWriter(schemaFile));
            writer.write(schemaToString());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        columnsName.clear();
        columnsType.clear();

        connected = false;
    }

    @Override
    public void upsert(WriteRequest wReq) throws IOException {
        for(Row row : wReq.getRows()){
            Vin vin = row.getVin();
            Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
            lock.lock();

            try {
                CarDataFileModel carDataFileModel = OUT_FILES.computeIfAbsent(vin, key -> new CarDataFileModel(vin, dataPath, columnsName, columnsNum));
                carDataFileModel.write(row);
                lastIndexFileModel.putLatVinRow(row);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        if(!initLastIndexed.get() || lastIndexFileModel.getLastIndexRowMap().isEmpty()){
            lastIndexFileModel.initLastIndexRowMap(initLastIndexed);
        }

        ArrayList<Row> ans = new ArrayList<>();
        for(Vin vin : pReadReq.getVins()){
            Row row = lastIndexFileModel.getLastRow(vin);
            if(Objects.nonNull(row)){
                Map<String, ColumnValue> filteredColumns = new HashMap<>();
                Map<String, ColumnValue> columns = row.getColumns();
                for(String key : pReadReq.getRequestedColumns()){
                    filteredColumns.put(key, columns.get(key));
                }
                ans.add(new Row(vin, row.getTimestamp(), filteredColumns));
            }
        }
        return ans;
    }

    @Override
    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        ArrayList<Row> ans = new ArrayList<>();
        Vin vin = trReadReq.getVin();
        Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
        lock.lock();

        try {
            CarDataFileModel carDataFileModel = OUT_FILES.computeIfAbsent(vin, key -> new CarDataFileModel(vin, dataPath, columnsName, columnsNum));
            ans = carDataFileModel.executeTimeRangeQuery(trReadReq);
        } finally {
            lock.unlock();
        }
        return ans;
    }

    private String schemaToString() {
        StringBuilder sb = new StringBuilder();
        sb.append(columnsNum);
        for (int i = 0; i < columnsNum; ++i) {
            sb.append(",")
                    .append(columnsName.get(i))
                    .append(",")
                    .append(columnsType.get(i));
        }
        return sb.toString();
    }

    private File getVinFilePath(Vin vin) {
        int folderIndex = vin.hashCode() % NUM_FOLDERS;
        File folder = new File(dataPath, String.valueOf(folderIndex));
        String vinStr = new String(vin.getVin(), StandardCharsets.UTF_8);
        File vinFile = new File(folder.getAbsolutePath(), vinStr);
        if (!folder.exists()) {
            folder.mkdirs();
        }
        if (!vinFile.exists()) {
            try {
                vinFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return vinFile;
    }
}
