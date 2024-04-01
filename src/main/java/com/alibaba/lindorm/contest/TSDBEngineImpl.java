//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.engine.NioTSDBEngineImpl;
import com.alibaba.lindorm.contest.structs.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class TSDBEngineImpl extends TSDBEngine {

  private final TSDBEngine tsdbEngine;
  /**
   * This constructor's function signature should not be modified.
   * Our evaluation program will call this constructor.
   * The function's body can be modified.
   */
  public TSDBEngineImpl(File dataPath) {
    super(dataPath);
    tsdbEngine = new NioTSDBEngineImpl(dataPath);
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