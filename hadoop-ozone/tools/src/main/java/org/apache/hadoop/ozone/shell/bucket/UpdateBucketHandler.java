/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.shell.bucket;

import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;

/**
 * create bucket handler.
 */
@Command(name = "update",
    description = "Updates parameter of the buckets")
public class UpdateBucketHandler extends BucketHandler {

  @Option(names = {"--spaceQuota", "-sq"},
      description = "Quota in bytes of the newly created volume (eg. 1GB)")
  private String quotaInBytes;

  @Option(names = {"--quota", "-q"},
      description = "Key counts of the newly created bucket (eg. 5)")
  private long quotaInCounts = OzoneConsts.QUOTA_RESET;

  /**
   * Executes create bucket.
   */
  @Override
  public void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    OzoneBucket bucket = client.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName);
    long spaceQuota = bucket.getQuotaInBytes();
    long countQuota = bucket.getQuotaInCounts();

    if (quotaInBytes != null && !quotaInBytes.isEmpty()) {
      spaceQuota = OzoneQuota.parseQuota(quotaInBytes,
          quotaInCounts).getQuotaInBytes();
    }
    if (quotaInCounts >= 0) {
      countQuota = quotaInCounts;
    }

    bucket.setQuota(OzoneQuota.getOzoneQuota(spaceQuota, countQuota));
    printObjectAsJson(bucket);
  }
}
