package org.philosophicas.flumenx

import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import kotlin.jvm.Throws


enum class DatumType(init: kotlin.Byte) {
    Byte(0.toByte()),
    Int(1.toByte()),
    Short(2.toByte()),
    Long(3.toByte()),
    Double(4.toByte()),
    Float(5.toByte()),
    String(6.toByte()),
}

/**
 * Datum class. Each datum is a object which encapsulate a data like a number or string.
 *
 * Datum structure:
 * ---------------
 *
 * Name             | Size (in bytes)
 * --------------------------
 * Engine version   | 1
 * Status           | 1
 * Record           | 4
 * Path hash        | 4
 * Path size (n)    | 4
 * Payload size (m) | 4
 * Path             | n
 * Payload          | m
 */
class Datum {

    /** Engin version */
    var version: Byte = 1

    /** Datum status */
    var status: Status = Status.Normal

    /**
     * Use for group some datum like a row index
     */
    var record = 0

    /** Hash value of path */
    var pathHash: Int = 0

    /** Size in bytes of path */
    var pathSize: Int = 0

    /** Size of payload */
    var payloadSize: Int = 0

    var path: String = ""
        get() = field
        set(value) {
            field = value
            pathHash = value.hashCode()
            pathSize = path.toByteArray().size
        }

    var payload: ByteArray = ByteArray(0)
        get() = field
        set(value) {
            field = value
            payloadSize = value.size
        }

    constructor()

    constructor(path: String) {
        this.path = path
    }

    companion object {

        /** Create a datum object from a InputStream */
        fun from(stream: InputStream): Datum? {

            val buffer = ByteBuffer.allocate(18)
            val byteArray = ByteArray(18)

            //Verify stream
            if (stream.available() < 18) {
                return null
            }

            val d = Datum()
            stream.read(byteArray)
            buffer.put(byteArray)

            // get version
            d.version = buffer.get()

            // get status
            d.status = Status.valueOf(buffer.get())

            //get record
            d.record = buffer.int

            //get path hash
            d.pathHash = buffer.int

            // Path size (n)    | 4
            d.pathSize = buffer.int

            //* Payload size (m) | 4
            d.payloadSize = buffer.int
            //* Path             | n

            val bufPath = ByteArray(d.pathSize)
            stream.read(bufPath)
            d.path = String(bufPath, Charsets.UTF_8)

            //* Payload          | m
            val bufPayload = ByteArray(d.payloadSize)
            stream.read(bufPayload)
            d.payload = bufPayload

            return d
        }

        fun from(value: Int, path: String): Datum {
            val d = Datum(path)
            d.payload = ByteBuffer.allocate(4).putInt(value).array()
            return d
        }

        fun from(value: Short, path: String): Datum {
            val d = Datum(path)
            d.payload = ByteBuffer.allocate(2).putShort(value).array()
            return d
        }

        fun from(value: Float, path: String): Datum {
            val d = Datum(path)
            d.payload = ByteBuffer.allocate(4).putFloat(value).array()
            return d
        }

        fun from(value: Char, path: String): Datum {
            val d = Datum(path)
            d.payload = ByteBuffer.allocate(2).putChar(value).array()
            return d
        }

        fun from(value: Long, path: String): Datum {
            val d = Datum(path)
            d.payload = ByteBuffer.allocate(8).putLong(value).array()
            return d
        }


        fun from(value: Double, path: String): Datum {
            val d = Datum(path)
            d.payload = ByteBuffer.allocate(8).putDouble(value).array()
            return d
        }

        fun from(value: ByteArray, path: String): Datum {
            val d = Datum(path)
            d.payload = value
            return d
        }

        fun from(value: String, path: String): Datum {
            val d = Datum(path)
            d.payload = value.toByteArray(Charsets.UTF_8)
            return d
        }

        fun from(value: Boolean, path: String): Datum {
            val d = Datum(path)
            d.payload = if (value) {
                byteArrayOf(1)
            } else {
                byteArrayOf(0)
            }
            return d
        }
    }

    /** Serialize this datum
     * @return Datum size */
    fun to(stream: OutputStream): Int {
        val datumSize = 18 + path.toByteArray(Charsets.UTF_8).size + payload.size
        val buffer = ByteBuffer.allocate(datumSize)

        //  * Engine version   | 1
        buffer.put(version)

        // * Status           | 1
        buffer.put(status.init)

        //  * Record           | 4
        buffer.putInt(record)

        // * Path hash        | 4
        buffer.putInt(pathHash)

        // * Path size (n)    | 4
        buffer.putInt(pathSize)

        // * Payload size (m) | 4
        buffer.putInt(payloadSize)

        // * Path             | n
        buffer.put(path.toByteArray(Charsets.UTF_8))

        //   * Payload          | m
        buffer.put(payload)

        return datumSize
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


}