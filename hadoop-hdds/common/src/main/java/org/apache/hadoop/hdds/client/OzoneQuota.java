/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.client;

import org.apache.hadoop.ozone.OzoneConsts;


/**
 * represents an OzoneQuota Object that can be applied to
 * a storage volume.
 */
public final class OzoneQuota {

  public static final String OZONE_QUOTA_BYTES = "BYTES";
  public static final String OZONE_QUOTA_KB = "KB";
  public static final String OZONE_QUOTA_MB = "MB";
  public static final String OZONE_QUOTA_GB = "GB";
  public static final String OZONE_QUOTA_TB = "TB";

  /** Quota Units.*/
  public enum Units {UNDEFINED, BYTES, KB, MB, GB, TB}

  // Quota to decide how many buckets or keys can be created.
  private long namespaceQuota;
  // Quota to decide how many storage space will be used in bytes.
  private long storagespaceQuota;
  private RawStorageSpaceQuota rawStoragespaceQuota;

  private static class RawStorageSpaceQuota {
    private Units unit;
    private long size;

    RawStorageSpaceQuota(Units unit, long size) {
      this.unit = unit;
      this.size = size;
    }

    public Units getUnit() {
      return unit;
    }

    public long getSize() {
      return size;
    }

    /**
     * Returns size in Bytes or -1 if there is no Quota.
     */
    public long sizeInBytes() {
      switch (this.unit) {
      case BYTES:
        return this.getSize();
      case KB:
        return this.getSize() * OzoneConsts.KB;
      case MB:
        return this.getSize() * OzoneConsts.MB;
      case GB:
        return this.getSize() * OzoneConsts.GB;
      case TB:
        return this.getSize() * OzoneConsts.TB;
      case UNDEFINED:
      default:
        return -1;
      }
    }

    @Override
    public String toString() {
      return size + " " + unit;
    }

  }

  /**
   * Returns size.
   *
   * @return long
   */
  public long getRawSize() {
    return this.rawStoragespaceQuota.getSize();
  }

  /**
   * Returns Units.
   *
   * @return Unit in MB, GB or TB
   */
  public Units getUnit() {
    return this.rawStoragespaceQuota.getUnit();
  }

  /**
   * Constructs a default Quota object.
   */
  private OzoneQuota() {
    this.namespaceQuota = OzoneConsts.QUOTA_COUNT_RESET;
    this.storagespaceQuota = OzoneConsts.MAX_QUOTA_IN_BYTES;
  }

  /**
   * Constructor for Ozone Quota.
   *
   * @param namespaceQuota long value
   * @param rawStoragespaceQuota RawStorageSpaceQuota value
   */
  private OzoneQuota(long namespaceQuota,
                     RawStorageSpaceQuota rawStoragespaceQuota) {
    this.namespaceQuota = namespaceQuota;
    this.rawStoragespaceQuota = rawStoragespaceQuota;
    this.storagespaceQuota = rawStoragespaceQuota.sizeInBytes();
  }

  /**
   * Formats a quota as a string.
   *
   * @param quota the quota to format
   * @return string representation of quota
   */
  public static String formatQuota(OzoneQuota quota) {
    return String.valueOf(quota.getRawSize())+ quota.getUnit();
  }

  /**
   * Parses a user provided string and returns the
   * Quota Object.
   *
   * @param storagespaceQuotaStr Storage space Quota String
   * @param namespaceQuota namespace Quota
   *
   * @return OzoneQuota object
   */
  public static OzoneQuota parseQuota(String storagespaceQuotaStr,
                                      long namespaceQuota) {

    if ((storagespaceQuotaStr == null) || (storagespaceQuotaStr.isEmpty())) {
      throw new IllegalArgumentException(
          "Quota string cannot be null or empty.");
    }

    String uppercase = storagespaceQuotaStr.toUpperCase()
        .replaceAll("\\s+", "");
    String size = "";
    int nSize;
    Units currUnit = Units.MB;
    boolean found = false;
    if (uppercase.endsWith(OZONE_QUOTA_MB)) {
      size = uppercase
          .substring(0, uppercase.length() - OZONE_QUOTA_MB.length());
      currUnit = Units.MB;
      found = true;
    }

    if (uppercase.endsWith(OZONE_QUOTA_KB)) {
      size = uppercase
          .substring(0, uppercase.length() - OZONE_QUOTA_KB.length());
      currUnit = Units.KB;
      found = true;
    }

    if (uppercase.endsWith(OZONE_QUOTA_GB)) {
      size = uppercase
          .substring(0, uppercase.length() - OZONE_QUOTA_GB.length());
      currUnit = Units.GB;
      found = true;
    }

    if (uppercase.endsWith(OZONE_QUOTA_TB)) {
      size = uppercase
          .substring(0, uppercase.length() - OZONE_QUOTA_TB.length());
      currUnit = Units.TB;
      found = true;
    }

    if (uppercase.endsWith(OZONE_QUOTA_BYTES)) {
      size = uppercase
          .substring(0, uppercase.length() - OZONE_QUOTA_BYTES.length());
      currUnit = Units.BYTES;
      found = true;
    }

    if (!found) {
      throw new IllegalArgumentException("Quota unit not recognized. " +
          "Supported values are BYTES, MB, GB and TB.");
    }

    nSize = Integer.parseInt(size);
    if (nSize < 0) {
      throw new IllegalArgumentException("Quota cannot be negative.");
    }

    return new OzoneQuota(namespaceQuota,
        new RawStorageSpaceQuota(currUnit, nSize));
  }


  /**
   * Returns OzoneQuota corresponding to size in bytes.
   *
   * @param quotaInBytes in bytes to be converted
   * @param quotaInCounts in counts to be converted
   *
   * @return OzoneQuota object
   */
  public static OzoneQuota getOzoneQuota(long quotaInBytes,
                                         long quotaInCounts) {
    long size;
    Units unit;
    if (quotaInBytes % OzoneConsts.TB == 0) {
      size = quotaInBytes / OzoneConsts.TB;
      unit = Units.TB;
    } else if (quotaInBytes % OzoneConsts.GB == 0) {
      size = quotaInBytes / OzoneConsts.GB;
      unit = Units.GB;
    } else if (quotaInBytes % OzoneConsts.MB == 0) {
      size = quotaInBytes / OzoneConsts.MB;
      unit = Units.MB;
    } else if (quotaInBytes % OzoneConsts.KB == 0) {
      size = quotaInBytes / OzoneConsts.KB;
      unit = Units.KB;
    } else {
      size = quotaInBytes;
      unit = Units.BYTES;
    }
    return new OzoneQuota(quotaInCounts, new RawStorageSpaceQuota(unit, size));
  }

  public long getNamespaceQuota() {
    return namespaceQuota;
  }

  public long getStoragespaceQuota() {
    return storagespaceQuota;
  }

  @Override
  public String toString() {
    return "Storagespace Quota: " + rawStoragespaceQuota.toString() + "\n" +
        "Namespace Quota: " + namespaceQuota;
  }
}
