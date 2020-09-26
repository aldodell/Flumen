package org.philosophicas.flumenx

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream


/**
 * Status enumeration
 */
enum class Status(var init: Byte) {
    Normal(0),
    Updated(1),
    Deleted(2);

    companion object {
        fun valueOf(init: Byte): Status {
            when (init.toInt()) {
                0 -> return Normal
                1 -> return Updated
                2 -> return Deleted
                else -> return Normal
            }
        }
    }
}

/** Data base engine */
class Engine {

    /** Engine version */
    val version = 1

    /** Databas file */
    private lateinit var file: File

    /** Map of path and records to look up last record
     * Key: hash's path
     * Value: Record
     * */
    val records = mutableMapOf<Int, Int>()

    /** Constuctor */
    constructor(file: File) {
        this.file = file
    }

    /** Constuctor */
    constructor(filename: String) {
        this.file = File(filename)
    }

    /** Detect records */
    fun updateRecords() {
        val fis = FileInputStream(file)
        while (fis.available() > 0) {
            Datum.from(fis, true)?.let { datum ->
                if (!records.containsKey(datum.pathHash)) {
                    records.put(datum.pathHash, datum.record)
                } else {
                    records[datum.pathHash]!!.let { record ->
                        if (datum.record > record) {
                            records.remove(datum.pathHash)
                            records[datum.pathHash] = datum.record
                        }
                    }
                }
            }
        }
        fis.close()
    }

    fun incrementRecord(pathHash: Int): Int {
        var r = 0
        if (records.containsKey(pathHash)) {
            r = records[pathHash]!!
            records.remove(pathHash)
            r += 1
            records[pathHash] = r
        } else {
            records[pathHash] = 0
        }
        return r
    }


    /** Add a RecordTemplate model object at end */
    fun <T : RecordTemplate> add(obj: T) {
        val fos = FileOutputStream(file, true)
        var record = -1
        obj.readProperties().forEach { datum ->
            if (record < 0) {
                record = incrementRecord(datum.pathHash)
            }
            datum.record = record
            fos.write(datum.bytes)
        }
        fos.flush()
        fos.close()
    }

    fun <T : RecordTemplate> add(objs: List<T>) {
        val fos = FileOutputStream(file, true)
        var record = -1
        var firstHash = 0
        objs.forEach { obj ->
            obj.readProperties().forEach { datum ->
                if (record < 0) {
                    firstHash = datum.pathHash
                    record = incrementRecord(firstHash)
                }
                datum.record = record
                fos.write(datum.bytes)
            }
            record = incrementRecord(firstHash)
        }
        fos.flush()
        fos.close()
    }

    fun <T : RecordTemplate> delete(objs: List<T>) {
        val file2 = File.createTempFile("flumen", "tmp")
        val fis = FileInputStream(file)
        val fos = FileOutputStream(file2)


        objs.forEach { obj ->
            val datums = obj.readProperties()
            var i = 0
            fis.reset()

            while (fis.available() > 18) {
                Datum.from(fis)?.let { datumFile ->
                    if (!datums.any { it.signature == datumFile.signature }) {
                        fos.write(datumFile.bytes)
                    } else {
                        i += 1
                    }

                }
                if (i == datums.size) break
            }
        }

        fos.flush()
        fos.close()
        fis.close()

        file.delete()
        file2.renameTo(file)

    }


    fun <T : RecordTemplate> update(objs: List<T>) {
        val file2 = File.createTempFile("flumen", "tmp")
        val fis = FileInputStream(file)
        val fos = FileOutputStream(file2)


        objs.forEach { obj ->
            val datums = obj.readProperties()
            var i = 0
            fis.reset()

            while (fis.available() > 18) {
                Datum.from(fis)?.let { datumFile ->
                    if (!datums.any { it.signature == datumFile.signature }) {
                        fos.write(datumFile.bytes)
                    } else {
                        datums.find { it.signature == datumFile.signature }?.let { d ->
                            fos.write(d.bytes)
                        }
                        i += 1
                    }

                }
                if (i == datums.size) break
            }
        }

        fos.flush()
        fos.close()
        fis.close()

        file.delete()
        file2.renameTo(file)

    }


}