package com.alibaba.lindorm.contest.data;


import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.engine.NioTSDBEngineImpl;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.lindorm.contest.common.Constants.VALUE_LENGTH;

/**
 * @author ashang
 * @date 2023/8/6 16:10
 * @description
 */
public class CarDataFileModel {
    private FileChannel fileChannel;
    private File dataPath;
    private Vin vin;

    private static final int NUM_FOLDERS = 300;
    private AtomicBoolean isInit = new AtomicBoolean(false);

    private ByteBuffer readByteBuffer = ByteBuffer.allocate(VALUE_LENGTH);
    private ByteBuffer readTimeRangeByteBuffer = ByteBuffer.allocate(VALUE_LENGTH * 4);

    private ByteBuffer writeByteBuffer = ByteBuffer.allocate(VALUE_LENGTH);

    private ArrayList<String> columnsName;

    public int columnsNum;

    public CarDataFileModel(Vin vin, File dataPath, ArrayList<String> columnsName, int columnsNum){
        this.vin = vin;
        this.dataPath = dataPath;
        this.columnsName = columnsName;
        this.columnsNum = columnsNum;
    }

    public synchronized void write(Row row) throws IOException {
        FileChannel fileChannel = getFileChannel(row);
        byte[] bytes = appendRowToBytes(row);
        writeByteBuffer.clear();
        writeByteBuffer.putInt(bytes.length).put(bytes);
        writeByteBuffer.flip();
        fileChannel.write(writeByteBuffer);
    }

    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        ArrayList<Row> ans = new ArrayList<>();
        Vin vin = trReadReq.getVin();

        try {
            FileChannel fileChannel = getFileChannel(vin);
            fileChannel.position(0);

            readTimeRangeByteBuffer.clear();

            int rowSize = -1;
            while(fileChannel.read(readTimeRangeByteBuffer) != -1){
                readTimeRangeByteBuffer.flip();
                if(rowSize == -1){
                    rowSize = readTimeRangeByteBuffer.getInt();
                }
                while(rowSize != -1 && readTimeRangeByteBuffer.remaining() != 0 && readTimeRangeByteBuffer.remaining() >= rowSize){
                    byte[] rowBytes = new byte[rowSize];
                    readTimeRangeByteBuffer.get(rowBytes);

                    Row row = readRowFromByteBuffer(ByteBuffer.wrap(rowBytes));

                    // check in timeRange
                    long timestamp = row.getTimestamp();
                    if(timestamp >= trReadReq.getTimeLowerBound() && timestamp < trReadReq.getTimeUpperBound()){
                        Map<String, ColumnValue> filterColumns = new HashMap<>();
                        Map<String, ColumnValue> columns = row.getColumns();

                        for(String key : trReadReq.getRequestedColumns()){
                            filterColumns.put(key, columns.get(key));
                        }
                        ans.add(new Row(vin, row.getTimestamp(), filterColumns));
                    }


                    // read next row
                    if(readTimeRangeByteBuffer.remaining() >= 4){
                        rowSize = readTimeRangeByteBuffer.getInt();
                    } else{
                        rowSize = -1;
                    }
                }
                readTimeRangeByteBuffer.compact();
            }
        } finally {

        }
        return ans;
    }

    public void close() throws IOException {
        if(this.fileChannel != null){
            this.fileChannel.close();
        }
    }

    private FileChannel getFileChannel(Vin vin) throws FileNotFoundException {
        if(isInit.get()){
            return fileChannel;
        }
        synchronized (this){
            if(isInit.get()){
                return fileChannel;
            }

            File vinFilePath = getVinFilePath(vin);
            fileChannel = new RandomAccessFile(vinFilePath, "rw").getChannel();
            isInit.compareAndSet(false, true);
        }
        return fileChannel;
    }

    private FileChannel getFileChannel(Row row) throws FileNotFoundException {
        if(isInit.get()){
            return fileChannel;
        }
        synchronized (this){
            if(isInit.get()){
                return fileChannel;
            }

            File vinFilePath = getVinFilePath(row.getVin());
            fileChannel = new RandomAccessFile(vinFilePath, "rw").getChannel();
            isInit.compareAndSet(false, true);
        }
        return fileChannel;
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
