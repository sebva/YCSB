package com.yahoo.ycsb.db

import ch.unine.anonymbe.api.*
import ch.unine.anonymbe.client.Client
import ch.unine.anonymbe.client.IndexedEnvelope
import ch.unine.anonymbe.storage.HybridTokenAwsMinio
import ch.unine.anonymbe.storage.Minio
import ch.unine.anonymbe.storage.WriterProxy
import com.yahoo.ycsb.ByteArrayByteIterator
import com.yahoo.ycsb.ByteIterator
import com.yahoo.ycsb.DB
import com.yahoo.ycsb.Status
import io.minio.ErrorCode
import io.minio.errors.ErrorResponseException
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.util.*

class AnonymBEStorage : DB() {
    private val apiUrl get() = properties["apiurl"] as? String ?: Api.DEFAULT_URL

    private val client by lazy {
        val minioUrl = properties["miniourl"] as? String ?: Minio.DEFAULT_ENDPOINT
        val writerProxyUrl = properties["writerproxyurl"] as? String ?: WriterProxy.DEFAULT_URL_TOKEN

        val storageClient = HybridTokenAwsMinio(minioEndpoint = minioUrl, writerProxyEndpoint = writerProxyUrl)

        Client(userId = USER_ID, apiUrl = apiUrl, storageClient = storageClient) { IndexedEnvelope(it) }
    }

    private val adminApi: AdminApi = Api.build(apiUrl)

    private lateinit var userKey: String

    override fun init() {
        val userObject = User(USER_ID)
        var createUserResult = adminApi.createUser(userObject).execute()

        if (!createUserResult.isReallySuccessful) {
            adminApi.deleteUser(userObject).execute()
            createUserResult = adminApi.createUser(userObject).execute()
        }
        val user = createUserResult.body()
                ?: throw Exception("createUser call failed: ${createUserResult.errorBody()?.string()}")
        userKey = user.user_key

        adminApi.addUserToGroup(UserGroup(USER_ID, GROUP_ID)).execute().throwExceptionIfNotReallySuccessful()

        client
    }

    override fun cleanup() {
        try {
            adminApi.deleteUser(User(USER_ID))
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    override fun insert(table: String?, filename: String, values: MutableMap<String, ByteIterator>): Status {
        val (symKey, envelope) = client.generateSymmetricKeyAndGetEnvelope(GROUP_ID)

        val byteOutput = ByteArrayOutputStream(values.map { Int.SIZE_BYTES * 2 + (it.key.length * 2) + it.value.bytesLeft().toInt() }.sum())
        val output = DataOutputStream(byteOutput)

        values.forEach { key, value ->
            output.writeInt(key.length)
            output.writeInt(value.bytesLeft().toInt())
            output.writeBytes(key)
            while (value.hasNext()) {
                output.writeByte(value.nextByte().toInt())
            }
        }
        output.flush()
        output.close()
        val data: ByteArray = byteOutput.toByteArray()

        return try {
            client.uploadToCloud(data, envelope, symKey, GROUP_ID, filename)
            Status.OK
        } catch (e: Exception) {
            e.printStackTrace()
            Status.ERROR
        }
    }

    override fun read(table: String?, filename: String, fields: MutableSet<String>?, result: MutableMap<String, ByteIterator>): Status = try {
        val data = client.retrieveFromCloud(userKey, GROUP_ID, filename)
        val input: ByteBuffer = ByteBuffer.wrap(data)
        while (input.hasRemaining()) {
            val keySize = input.getInt()
            val valueSize = input.getInt()

            val keyBuffer = ByteArray(keySize)
            input.get(keyBuffer)
            val key = String(keyBuffer)
            if (fields != null && key !in fields) {
                input.position(input.position() + valueSize)
                continue
            }

            val valueBuffer = ByteArray(valueSize)
            input.get(valueBuffer)
            result.put(key, ByteArrayByteIterator(valueBuffer))
        }
        Status.OK
    } catch (e: ErrorResponseException) {
        if (e.errorResponse()?.errorCode() == ErrorCode.NO_SUCH_KEY) {
            Status.NOT_FOUND
        } else {
            e.printStackTrace()
            Status.ERROR
        }
    } catch (e: Exception) {
        e.printStackTrace()
        Status.ERROR
    }

    override fun delete(table: String?, filename: String): Status = try {
        client.deleteFromCloud(GROUP_ID, filename)
        Status.OK
    } catch (e: Exception) {
        e.printStackTrace()
        Status.ERROR
    }

    override fun scan(table: String?, startkey: String?, recordcount: Int, fields: MutableSet<String>?, result: Vector<HashMap<String, ByteIterator>>?): Status =
            Status.NOT_IMPLEMENTED

    override fun update(table: String?, key: String?, values: MutableMap<String, ByteIterator>?): Status = Status.NOT_IMPLEMENTED

    companion object {
        const val USER_ID = "macrouser"
        const val GROUP_ID = "macrogroup"
    }
}
