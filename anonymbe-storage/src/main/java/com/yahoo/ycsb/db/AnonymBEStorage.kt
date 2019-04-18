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
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.HashMap

class AnonymBEStorage : DB() {
    private lateinit var client: Client

    override fun init() {
        val tid = tidCounter.getAndIncrement()

        val apiUrl = properties["apiurl"] as? String ?: Api.DEFAULT_URL
        val minioUrl = properties["miniourl"] as? String ?: Minio.DEFAULT_ENDPOINT
        val writerProxyUrl = properties["writerproxyurl"] as? String ?: WriterProxy.DEFAULT_URL_TOKEN

        val storageClient = Minio(minioUrl)
        try {
            storageClient.createBucketIfNotExists(GROUP_ID)
        } catch (e: Exception) {
        }

        val userId = "$USER_ID_PREFIX$tid"
        val userKey = ByteArray(USER_KEY_LENGTH).also {
            userId.toByteArray().copyInto(it)
        }
        client = Client(userId, userKey, apiUrl, storageClient) { IndexedEnvelope(it) }

        println("thread $tid ready")
    }

    override fun insert(table: String?, filename: String, values: MutableMap<String, ByteIterator>): Status {
        val (symKey, envelope) = client.generateSymmetricKeyAndGetEnvelope(GROUP_ID)

        val byteOutput = ByteArrayOutputStream(values.map { Int.SIZE_BYTES * 2 + (it.key.length * 2) + it.value.bytesLeft().toInt() }.sum())
        val output = DataOutputStream(byteOutput)

        values.forEach { (key, value) ->
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

    /**
     * Update does read-modify-write!
     */
    override fun update(table: String?, filename: String, updatedValues: MutableMap<String, ByteIterator>): Status {
        val values = HashMap<String, ByteIterator>()
        val readStatus = read(table, filename, null, values)
        if (readStatus != Status.OK) {
            return readStatus
        }

        values += updatedValues

        return insert(table, filename, values)
    }

    override fun read(table: String?, filename: String, fields: MutableSet<String>?, result: MutableMap<String, ByteIterator>): Status = try {
        val data = client.retrieveFromCloud(GROUP_ID, filename)
        val input: ByteBuffer = ByteBuffer.wrap(data)
        while (input.hasRemaining()) {
            val keySize = input.int
            val valueSize = input.int

            val keyBuffer = ByteArray(keySize)
            input.get(keyBuffer)
            val key = String(keyBuffer)
            if (fields != null && key !in fields) {
                input.position(input.position() + valueSize)
                continue
            }

            val valueBuffer = ByteArray(valueSize)
            input.get(valueBuffer)
            result[key] = ByteArrayByteIterator(valueBuffer)
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
        // For some mysterious reason, in < 5% of the cases, the client does not find the user's key in the envelope
        if (e.message != "User key not in envelope") {
            e.printStackTrace()
        }
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

    companion object {
        const val USER_ID_PREFIX = "macrouser"
        const val GROUP_ID = "field0"
        const val USER_KEY_LENGTH = 32 // Synchronize with AnonymBE service
        @JvmStatic
        val tidCounter = AtomicInteger(0)
    }
}
