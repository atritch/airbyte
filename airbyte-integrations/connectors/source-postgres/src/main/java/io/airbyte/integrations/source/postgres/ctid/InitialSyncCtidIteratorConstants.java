/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.ctid;

public class InitialSyncCtidIteratorConstants {

  public static final int MAX_ALLOWED_RESYNCS = 5;
  // Reduced from 1GB to 256MB to reduce memory pressure and transaction duration
  // This results in more frequent commits but safer memory usage
  public static final double QUERY_TARGET_SIZE_GB = 0.25;

  private static final double MEGABYTE = Math.pow(1024, 2);
  public static final double GIGABYTE = MEGABYTE * 1024;

  /**
   * Constants to be used for tests
   */
  private static final double ONE_KILOBYTE = 1024;
  public static final double EIGHT_KB = ONE_KILOBYTE * 8;
  public static final String USE_TEST_CHUNK_SIZE = "use_test_chunk_size";

}
