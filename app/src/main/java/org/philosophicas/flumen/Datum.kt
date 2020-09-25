package org.philosophicas.flumen

import java.nio.ByteBuffer



enum class DatumType {
    Integer,
    Float,
    Double,
    Long,
    Short,
    Boolean,
    String,
    Char,
    Bytes,

}


/**
 * Interface for Datum generic object
 */
interface IDatum {

    /** To identify engine requirement */
    var version: Byte // 1

    /** For some reasons. For instance, mark this datum as deleted*/
    var status: Status // 1

    /** Path hash to improve search performance */
    var pathHash: Int // 4

    /** Record index, like row index on a table */
    var record: Int //4

    /** Real path size */
    var pathSize: Int // 4

    /** Return payload size */
    var payloadSize: Int

    /** Path identifier of this datum */
    var path: String // n

    /** Binary representation of util data */
    var payload: ByteArray //m

    /** Convert this datum to bytes, to handle storage*/
    val binary: ByteArray

    /** Package size */
    var datumSize: Int  // 4
}


/**
 * This class wrap a package or datum.
 * Each datum have a payload(data of interest) and metadata.

 */
open class Datum(path: String) : IDatum {

    private var mPathSize = 0
    private var mPayloadSize = 0
    private var mPayload = byteArrayOf(0)
    private var mPath = ""


    companion object {
        fun from(value: Int, path: String): Datum {
            var d = Datum(path)
            d.payload = ByteBuffer.allocate(4).putInt(value).array()
            return d
        }

        fun from(value: Short, path: String): Datum {
            var d = Datum(path)
            d.payload = ByteBuffer.allocate(2).putShort(value).array()
            return d
        }

        fun from(value: Float, path: String): Datum {
            var d = Datum(path)
            d.payload = ByteBuffer.allocate(4).putFloat(value).array()
            return d
        }

        fun from(value: Char, path: String): Datum {
            var d = Datum(path)
            d.payload = ByteBuffer.allocate(2).putChar(value).array()
            return d
        }

        fun from(value: Long, path: String): Datum {
            var d = Datum(path)
            d.payload = ByteBuffer.allocate(8).putLong(value).array()
            return d
        }


        fun from(value: Double, path: String): Datum {
            var d = Datum(path)
            d.payload = ByteBuffer.allocate(8).putDouble(value).array()
            return d
        }

        fun from(value: ByteArray, path: String): Datum {
            var d = Datum(path)
            d.payload = value
            return d
        }

        fun from(value: String, path: String): Datum {
            var d = Datum(path)
            d.payload = value.toByteArray(Charsets.UTF_8)
            return d
        }

        fun from(value: Boolean, path: String): Datum {
            var d = Datum(path)
            d.payload = if (value) {
                byteArrayOf(1)
            } else {
                byteArrayOf(0)
            }
            return d
        }


    }

    override var path: String
        get() = mPath
        set(value) {
            mPath = value
            mPathSize = mPath.toByteArray(Charsets.UTF_8).size
        }

    /** Return payload size */
    override var payloadSize: Int
        get() = mPayloadSize
        set(value) {
            mPayloadSize = value
        }


    override var version: Byte = 0
        get() = ENGINE_VERSION.toByte()

    override var pathHash: Int = 0
        get() = path.hashCode()

    override var pathSize: Int
        get() = mPathSize
        set(value) {
            mPathSize = value
        }

    override var payload: ByteArray
        get() {
            return mPayload
        }
        set(value) {
            mPayload = value
            payloadSize = value.size
        }


    /** For some reasons. For instance, mark this datum as deleted*/
    override var status = Status.NORMAL

    /** Record index, like row index on a table */
    override var record: Int = 0

    override var datumSize: Int = 0
        get() {

            return 1 + // version
                    1 + // status
                    4 + // datum size
                    4 + // path hash
                    4 + // path size
                    4 + // record
                    mPathSize + //
                    mPayloadSize
        }

    /** Convert this datum to bytes, to handle storage*/
    override var binary: ByteArray
        get() {
            var b = ByteBuffer.allocate(datumSize)
            b.put(version)
            b.put(status.value)
            b.putInt(pathHash)
            b.putInt(record)
            b.putInt(mPathSize)
            b.putInt(mPayloadSize)
            b.put(path.toByteArray(Charsets.UTF_8))
            b.put(payload)
            return b.array()
        }
        set(value) {
            var b = ByteBuffer.wrap(value)
            version = b.get()
            status.value = b.get()
            pathHash = b.int
            record = b.int
            pathSize = b.int
            payloadSize = b.int
            var pathBuffer = ByteArray(pathSize)
            var payloadBuffer = ByteArray(payloadSize)
            b.get(pathBuffer)
            b.get(payloadBuffer)
            path = String(pathBuffer, Charsets.UTF_8)
            payload = payloadBuffer

        }

    /**
     * Base64 conversion facility
     */
    var base64: String
        get() {
            return Base64().encode(binary)
        }
        set(value) {
            binary = Base64().decode(value)
        }


    val int: Int
        get() {
            return ByteBuffer.wrap(payload).int
        }

    val short: Short
        get() {
            return ByteBuffer.wrap(payload).short
        }

    val float: Float
        get() {
            return ByteBuffer.wrap(payload).float
        }

    val char: Char
        get() {
            return ByteBuffer.wrap(payload).char
        }

    val double: Double
        get() {
            return ByteBuffer.wrap(payload).double
        }

    val long: Long
        get() {
            return ByteBuffer.wrap(payload).long
        }


    val string: String
        get() {
            return String(payload, Charsets.UTF_8)
        }

    val boolean: Boolean
        get() {
            return payload[0] == 1.toByte()
        }


    val byteArray: ByteArray
        get() = payload


    init {
        mPath = path
        mPathSize = mPath.toByteArray(Charsets.UTF_8).size
    }


}




