/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.jdbc.streaming;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class TwoStageSizeEstimatorTest {

  @Test
  public void testDelegationSwitch() {
    final TwoStageSizeEstimator sizeEstimator = TwoStageSizeEstimator.getInstance();
    for (int i = 0; i < FetchSizeConstants.INITIAL_SAMPLE_SIZE; ++i) {
      sizeEstimator.accept("1");
      assertTrue(sizeEstimator.getDelegate() instanceof InitialSizeEstimator);
    }
    // delegation is changed after initial sampling
    for (int i = 0; i < 3; ++i) {
      sizeEstimator.accept("1");
      assertTrue(sizeEstimator.getDelegate() instanceof SamplingSizeEstimator);
    }
  }

  @Test
  public void testGetTargetBufferByteSize() {
    assertEquals(FetchSizeConstants.MIN_BUFFER_BYTE_SIZE,
        TwoStageSizeEstimator.getTargetBufferByteSize(null));
    assertEquals(FetchSizeConstants.MIN_BUFFER_BYTE_SIZE,
        TwoStageSizeEstimator.getTargetBufferByteSize(Long.MAX_VALUE));
    assertEquals(FetchSizeConstants.MIN_BUFFER_BYTE_SIZE,
        TwoStageSizeEstimator.getTargetBufferByteSize(FetchSizeConstants.MIN_BUFFER_BYTE_SIZE - 10L));
    assertEquals(FetchSizeConstants.MAX_BUFFER_BYTE_SIZE,
        TwoStageSizeEstimator.getTargetBufferByteSize(
            Math.round(FetchSizeConstants.MAX_BUFFER_BYTE_SIZE / FetchSizeConstants.TARGET_BUFFER_SIZE_RATIO + 10L)));
  }

}
