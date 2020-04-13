/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.key;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;

/**
 * Tests OmKeyDelete request.
 */
public class TestOMKeyDeleteRequest extends TestOMKeyRequest {

  @Test
  public void testPreExecute() throws Exception {
    doPreExecute(createDeleteKeyRequest());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteKeyRequest());

    OMKeyDeleteRequest omKeyDeleteRequest =
        new OMKeyDeleteRequest(modifiedOmRequest);

    // Add volume, bucket and key entries to OM DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);

    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    // As we added manually to key table.
    Assert.assertNotNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
        100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    // Now after calling validateAndUpdateCache, it should be deleted.

    omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    Assert.assertNull(omKeyInfo);
  }

  @Test
  public void testValidateAndUpdateCacheWithKeyNotFound() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteKeyRequest());

    OMKeyDeleteRequest omKeyDeleteRequest =
        new OMKeyDeleteRequest(modifiedOmRequest);

    // Add only volume and bucket entry to DB.
    // In actual implementation we don't check for bucket/volume exists
    // during delete key.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteKeyRequest());

    OMKeyDeleteRequest omKeyDeleteRequest =
        new OMKeyDeleteRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteKeyRequest());

    OMKeyDeleteRequest omKeyDeleteRequest =
        new OMKeyDeleteRequest(modifiedOmRequest);

    TestOMRequestUtils.addVolumeToDB(volumeName, omMetadataManager);

    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
            omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testReplayRequest() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteKeyRequest());

    OMKeyDeleteRequest omKeyDeleteRequest =
        new OMKeyDeleteRequest(modifiedOmRequest);

    // Add volume, bucket and key entries to OM DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    TestOMRequestUtils.addKeyToTableAndCache(volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, 1L, omMetadataManager);

    // Delete the key manually. Lets say the Delete Requests
    // TransactionLogIndex is 10.
    long deleteTrxnLogIndex = 10L;
    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
    TestOMRequestUtils.deleteKey(ozoneKey, omMetadataManager, 10L);

    // Create the same key again with TransactionLogIndex > Delete requests
    // TransactionLogIndex
    TestOMRequestUtils.addKeyToTableAndCache(volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, 20L, omMetadataManager);

    // Replay the original DeleteRequest.
    OMClientResponse omClientResponse = omKeyDeleteRequest
        .validateAndUpdateCache(ozoneManager, deleteTrxnLogIndex,
            ozoneManagerDoubleBufferHelper);

    // Replay should result in Replay response
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.REPLAY,
        omClientResponse.getOMResponse().getStatus());
  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOmRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {

    OMKeyDeleteRequest omKeyDeleteRequest =
        new OMKeyDeleteRequest(originalOmRequest);

    OMRequest modifiedOmRequest = omKeyDeleteRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  /**
   * Create OMRequest which encapsulates DeleteKeyRequest.
   * @return OMRequest
   */
  private OMRequest createDeleteKeyRequest() {
    List<String> keyNameList = new ArrayList<>();
    keyNameList.add(keyName);
    KeyArgs keyArgs = KeyArgs.newBuilder().setBucketName(bucketName)
        .setVolumeName(volumeName).addAllKeyNameList(keyNameList).build();

    DeleteKeyRequest deleteKeyRequest =
        DeleteKeyRequest.newBuilder().setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder().setDeleteKeyRequest(deleteKeyRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}
