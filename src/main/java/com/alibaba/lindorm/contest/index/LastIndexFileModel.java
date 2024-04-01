package com.alibaba.lindorm.contest.index;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.common.Constants;
import com.alibaba.lindorm.contest.engine.NioTSDBEngineImpl;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author ashang
 * @date 2023/9/1 19:00
 * @description
 */
public class LastIndexFileModel {
    private File baseDataPath;
    private File lastIndexFile;
    public static final String lastIndexFileName = "last_index";

    private static final ConcurrentMap<Vin, Lock> VIN_LOCKS = new ConcurrentHashMap<>();

    private ByteBuffer lastIndexButeBuffer = ByteBuffer.allocate(Constants.VALUE_LENGTH * 4);

    private FileChannel fileChannel;

    public ArrayList<String> columnsName;

    public int columnsNum;

    private Map<Vin, Row> lastIndexRowMap = new ConcurrentHashMap<>((int)(30000 * 1.5));

    public LastIndexFileModel(File dataPath, ArrayList<String> columnsName, int columnsNum){
        this.columnsName = columnsName;
        this.columnsNum = columnsNum;
        this.baseDataPath = dataPath;
        this.lastIndexFile = new File(dataPath, lastIndexFileName);
        if(!lastIndexFile.exists()){
            try {
                lastIndexFile.createNewFile();
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        try {
            fileChannel = new RandomAccessFile(lastIndexFile, "rw").getChannel();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public Map<Vin, Row> getLastIndexRowMap(){
        return this.lastIndexRowMap;
    }

    public void putLatVinRow(Row row){
        Vin vin = row.getVin();
        Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
        lock.lock();

        try {
            if(row != null && row.getVin() != null){
                Row oldRow = lastIndexRowMap.computeIfAbsent(vin, key -> row);
                if(oldRow == row){
                    return;
                }
                long oldTimestamp = oldRow.getTimestamp();
                long newTimestamp = row.getTimestamp();
                if(newTimestamp > oldTimestamp){
                    lastIndexRowMap.put(vin, row);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public Row getLastRow(Vin vin){
        return lastIndexRowMap.get(vin);
    }

    public synchronized void initLastIndexRowMap(AtomicBoolean initLastIndexed){
        if(initLastIndexed.get() && !lastIndexRowMap.isEmpty()){
            return;
        }

        try {
            lastIndexButeBuffer.clear();
            int rowSize = -1;
            while(fileChannel.read(lastIndexButeBuffer) != -1){
                lastIndexButeBuffer.flip();
                if(rowSize == -1){
                    rowSize = lastIndexButeBuffer.getInt();
                }
                while(rowSize != -1 && lastIndexButeBuffer.remaining() != 0 && lastIndexButeBuffer.remaining() >= rowSize){
                    byte[] rowBytes = new byte[rowSize];
                    lastIndexButeBuffer.get(rowBytes);

                    Row row = readRowFromByteBuffer(ByteBuffer.wrap(rowBytes));
                    lastIndexRowMap.put(row.getVin(), row);

                    if(lastIndexButeBuffer.remaining() >= 4){
                        rowSize = lastIndexButeBuffer.getInt();
                    } else{
                        rowSize = -1;
                    }
                }
                lastIndexButeBuffer.compact();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        initLastIndexed.set(true);
    }

    public void flushLastIndexRowMap(){
        try {
            for(Map.Entry<Vin, Row> entry : lastIndexRowMap.entrySet()){
                Row row = entry.getValue();
                byte[] bytes = appendRowToBytes(row);
                int length = bytes.length;
                lastIndexButeBuffer.clear();
                lastIndexButeBuffer.putInt(length).put(bytes);
                lastIndexButeBuffer.flip();
                fileChannel.write(lastIndexButeBuffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private byte[] appendRowToBytes(Row row) {
        if (row.getColumns().size() != columnsNum) {
            System.err.println("Cannot write a non-complete row with columns' num: [" + row.getColumns().size() + "]. ");
            System.err.println("There are [" + columnsNum + "] rows in total");
            throw new RuntimeException();
        }

        try {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(byteOut);
            dataOut.writeLong(row.getTimestamp());
            dataOut.writeInt(row.getVin().getVin().length);
            dataOut.write(row.getVin().getVin());

            for (int i = 0; i < columnsNum; ++i) {
                String cName = columnsName.get(i);
                ColumnValue cVal = row.getColumns().get(cName);
                switch (cVal.getColumnType()) {
                    case COLUMN_TYPE_STRING:
                        ByteBuffer stringByteBuffer = cVal.getStringValue();
                        dataOut.writeInt(stringByteBuffer.remaining());
                        dataOut.write(stringByteBuffer.array());
                        break;
                    case COLUMN_TYPE_INTEGER:
                        dataOut.writeInt(cVal.getIntegerValue());
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        dataOut.writeDouble(cVal.getDoubleFloatValue());
                        break;
                    default:
                        throw new IllegalStateException("Invalid column type");
                }
            }
            dataOut.flush();
            dataOut.close();
            byteOut.close();
            return byteOut.toByteArray();
        } catch (IOException e) {
            System.err.println("Error writing row to file");
            throw new RuntimeException(e);
        }
    }

    private Row readRowFromByteBuffer(ByteBuffer rowByteBuffer) {
        try {
            long timestamp = rowByteBuffer.getLong();
            int vinLength = rowByteBuffer.getInt();
            byte[] vinBytes = new byte[vinLength];
            rowByteBuffer.get(vinBytes);
            Vin vin = new Vin(vinBytes);

            Map<String, ColumnValue> columns = new HashMap<>();

            for (int cI = 0; cI < columnsNum; ++cI) {
                String cName = columnsName.get(cI);
                ColumnValue.ColumnType cType = NioTSDBEngineImpl.columnsType.get(cI);
                ColumnValue cVal;
                switch (cType) {
                    case COLUMN_TYPE_INTEGER:
                        int intVal = rowByteBuffer.getInt();
                        cVal = new ColumnValue.IntegerColumn(intVal);
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        double doubleVal = Double.longBitsToDouble(rowByteBuffer.getLong());
                        cVal = new ColumnValue.DoubleFloatColumn(doubleVal);
                        break;
                    case COLUMN_TYPE_STRING:
                        cVal = new ColumnValue.StringColumn(CommonUtils.readString(rowByteBuffer));
                        break;
                    default:
                        throw new IllegalStateException("Undefined column type, this is not expected");
                }
                columns.put(cName, cVal);
            }
            return new Row(vin, timestamp, columns);
        } catch (IOException e) {
            System.err.println("Error reading row from stream");
            throw new RuntimeException(e);
        }
    }
}
