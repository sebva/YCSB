package com.yahoo.ycsb.db

import ch.unine.anonymbe.api.*
import com.yahoo.ycsb.ByteIterator
import com.yahoo.ycsb.DB
import com.yahoo.ycsb.Status
import retrofit2.Response
import java.util.*

private val <T : Result> Response<T>.ycsbStatus: Status
    get() = when {
        this.isReallySuccessful -> Status.OK
        else -> {
            println(this)
            println(errorBody()?.string())
            Status.ERROR
        }
    }

private operator fun ByteIterator.rem(modulo: Int): Int {
    var sum = 0
    while (hasNext()) {
        sum += nextByte() % modulo
    }
    return sum % modulo
}

class AnonymBEAdmin : DB() {
    private val service by lazy<AdminApi> {
        val url = properties["apiurl"]
        if (url is String) {
            Api.build(url)
        } else {
            Api.build()
        }
    }

    override fun init() {
        service
    }

    override fun scan(table: String?, startkey: String?, recordcount: Int, fields: MutableSet<String?>?, result: Vector<HashMap<String?, ByteIterator?>?>?): Status =
            Status.NOT_IMPLEMENTED

    override fun insert(table: String?, key: String, values: MutableMap<String, ByteIterator>): Status {
        val resultCreateUser = service.createUser(User(key)).execute()

        val statusUpdateGroups = update(table, key, values)
        return if (statusUpdateGroups.isOk) {
            resultCreateUser.ycsbStatus
        } else {
            statusUpdateGroups
        }
    }

    override fun update(table: String?, key: String, values: MutableMap<String, ByteIterator>): Status {
        val errors = values.entries.parallelStream().map {
            val userGroup = UserGroup(key, it.key)
            val request = if (it.value % 2 == 0) {
                service.addUserToGroup(userGroup)
            } else {
                service.deleteUserFromGroup(userGroup)
            }
            return@map request.execute()
        }.filter {
            !it.isReallySuccessful
        }.count()

        return if (errors == 0L) {
            Status.OK
        } else {
            Status.ERROR
        }
    }

    override fun read(table: String?, key: String?, fields: MutableSet<String>?, result: MutableMap<String, ByteIterator>?): Status =
            Status.NOT_IMPLEMENTED

    override fun delete(table: String?, key: String) = service.deleteUser(User(key)).execute().ycsbStatus
}
