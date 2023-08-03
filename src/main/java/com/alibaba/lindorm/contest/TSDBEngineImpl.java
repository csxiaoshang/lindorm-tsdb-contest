//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.engine.TSDBEngineDemoImpl;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;

import javax.print.attribute.standard.Finishings;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TSDBEngineImpl extends TSDBEngine {

  private final TSDBEngine tsdbEngine;
  /**
   * This constructor's function signature should not be modified.
   * Our evaluation program will call this constructor.
   * The function's body can be modified.
   */
  public TSDBEngineImpl(File dataPath) {
    super(dataPath);
    tsdbEngine = new TSDBEngineDemoImpl(dataPath);
  }

  @Override
  public void connect() throws IOException {
    tsdbEngine.connect();
  }

  @Override
  public void createTable(String tableName, Schema schema) throws IOException {
    tsdbEngine.createTable(tableName, schema);
  }

  @Override
  public void shutdown() {
    tsdbEngine.shutdown();
  }

  @Override
  public void upsert(WriteRequest wReq) throws IOException {
    tsdbEngine.upsert(wReq);
  }

  @Override
  public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
    return tsdbEngine.executeLatestQuery(pReadReq);
  }

  @Override
  public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
    return tsdbEngine.executeTimeRangeQuery(trReadReq);
  }

}