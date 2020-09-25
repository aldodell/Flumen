package org.philosophicas.flumen

import android.content.Context
import java.io.*
import java.nio.ByteBuffer
import kotlin.collections.ArrayList
import kotlin.reflect.KClass
import kotlin.reflect.full.createInstance


/** Database version */
const val ENGINE_VERSION = 1

enum class Status(var value: Byte) {
    NORMAL(0), UPDATE(1), DELETED(2)
}

/**
 * Front end to database engine.
 */
class Engine private constructor() {

    /** Data base file */
    lateinit var file: File

    data class IndexRecord(var path: String, var record: Int)

    /** IndexRecord list */
    val indexRecordTable = ArrayList<IndexRecord>()


    /**
     * @param context Context object (like an activity)
     * @fileName Data base file name
     * @constructor Create Engine object with a filename place on app data directory
     *
     *  */
    constructor(context: Context, fileName: String) : this() {
        file = File(context.filesDir, fileName)
        prepare()
    }

    /**
     * @file Data base file. Is a File object, not a string file name.
     * @constructor Create Engine object with a filename place on app data directory
     *
     *  */
    constructor(dataBasefile: File) : this() {
        file = dataBasefile
        prepare()
    }


    /**
     * Initial process requires to start database use
     */
    private fun prepare() {
        //Create database file if do not exists
        if (!file.exists()) {
            file.createNewFile()
        } else {
            analyze()
        }
    }

    /**
     * This method do these things:
     * 1) Open database file
     * 2) Read each datum object
     * 3) Look on it's recordIndexTable if exists the path of actual datum.
     * 4) If exits get the top value and register, if don't exits create a entry on recordIndexTable.
     */
    fun analyze() {
        indexRecordTable.clear()
        val fis = FileInputStream(file)

        var datum: Datum? = null
        do {
            datum = read(fis)
            if (datum != null) {
                val path = datum.path
                val record = datum.record
                val indexRecord = indexRecordTable.firstOrNull { it.path == path }
                if (indexRecord == null) {
                    indexRecordTable.add(IndexRecord(path, 0))
                } else {
                    var i = indexRecordTable.indexOf(indexRecord)
                    if (record > indexRecordTable[i].record) {
                        indexRecordTable[i].record = datum.record
                    }
                }
            }

        } while (datum != null)
    }


    /**
     * Write a unique Datum object
     * @param stream OutputStream object which will have bianary data.
     * @param datum Datum object to be processed.
     */
    private fun write(stream: OutputStream, datum: Datum) {
        var baos = ByteArrayOutputStream()
        baos.write(datum.binary)
        baos.writeTo(stream)
        baos.flush()
        baos.close()
    }


    /**
     * Change status property on a specific datum
     * @return true if found a datum with this signature and was change it
     */
    fun changeDatumStatus(path: String, record: Int, newStatus: Status): Boolean? {

        val raf = RandomAccessFile(file, "rw")
        var i = 0
        val ba = ByteArray(18)
        var buffer = ByteBuffer.allocate(18)
        var payloadSize = 0
        var pathSize = 0
        var pathHash = 0
        var mRecord = 0
        var result = false


        /*
        1   version         0
        1   status          1
        4   path hash       2,5
        4   record          6,9
        4   path size       10,13
        4   payload size    14,17
         */

        while (i < raf.length()) {
            raf.read(ba)

            buffer.clear()
            buffer = buffer.put(ba)
            buffer.position(0)

            //Version
            buffer.get()

            //Satus
            buffer.get()

            //Path hash
            pathHash = buffer.int
            // Record
            mRecord = buffer.int
            // path size
            pathSize = buffer.int
            //payload size
            payloadSize = buffer.int


            if (pathHash == path.hashCode() && mRecord == record) {
                raf.seek((i + 1).toLong())
                raf.writeByte(newStatus.value.toInt())
                result = true
                break
            }
            raf.skipBytes(pathSize + payloadSize)
            i += ba.size + pathSize + payloadSize
        }
        raf.close()
        return result
    }


    /**
     * Change payload on a specific datum.
     * Will throw PayloadSizeOverflowException if new payload size
     * is greater than original payload size. This is for avoid database
     * file corruption.
     * If you require to change payload to more big one do like this:
     * Append a new datum object with same signature (path and record).
     * Mark to "DELETE" the original datum status property.
     * @return true if found a datum with this signature and was change it
     */
    @Throws(PayloadSizeOverflowException::class)
    fun changeDatumPayload(path: String, record: Int, payload: ByteArray): Boolean {
        val raf = RandomAccessFile(file, "rw")
        var i = 0
        val ba = ByteArray(18)
        var buffer = ByteBuffer.allocate(18)
        var payloadSize = 0
        var pathSize = 0
        var pathHash = 0
        var mRecord = 0
        var result = false

        /*
        1   version         0
        1   status          1
        4   path hash       2,5
        4   record          6,9
        4   path size       10,13
        4   payload size    14,17
         */

        while (i < raf.length()) {
            raf.read(ba)

            buffer.clear()
            buffer = buffer.put(ba)
            buffer.position(0)

            //Version
            buffer.get()

            //Satus
            buffer.get()

            //Path hash
            pathHash = buffer.int
            // Record
            mRecord = buffer.int
            // path size
            pathSize = buffer.int
            //payload size
            payloadSize = buffer.int

            if (pathHash == path.hashCode() && mRecord == record) {
                if (payload.size > payloadSize) {
                    throw PayloadSizeOverflowException(payloadSize)
                }

                raf.seek((i + ba.size + pathSize).toLong())
                raf.write(payload)
                result = true
                break
            }
            raf.skipBytes(pathSize + payloadSize)
            i += ba.size + pathSize + payloadSize
        }
        raf.close()
        return result
    }


    /**
     * Read next Datum object. Return null if catch a exception (like end of file found.)
     * @param stream Stream to be read.
     */
    fun read(stream: InputStream): Datum? {
        try {
            if (stream.available() < 18) return null

            var d = Datum("")
            var b1 = ByteArray(1)
            var b4 = ByteArray(4)

            //Get version
            stream.read(b1)
            d.version = b1.first()

            //get status
            stream.read(b1)
            d.status.value = b1.first()

            //Get path hash
            stream.read(b4)
            d.pathHash = ByteBuffer.wrap(b4).int

            //Get  record
            stream.read(b4)
            d.record = ByteBuffer.wrap(b4).int

            //Get  pathSize
            stream.read(b4)
            val pathSize = ByteBuffer.wrap(b4).int
            d.pathSize = pathSize

            //Get  payload size
            stream.read(b4)
            val payloadSize = ByteBuffer.wrap(b4).int
            d.payloadSize = payloadSize

            //Get  path
            val bufferPath = ByteArray(pathSize)
            stream.read(bufferPath)
            d.path = bufferPath.toString(Charsets.UTF_8)

            //Get payload
            val bufferPayload = ByteArray(payloadSize)
            stream.read(bufferPayload)
            d.payload = bufferPayload

            return d


        } catch (ex: Exception) {
            ex.printStackTrace()
        }
        return null
    }

    /**
     * Save a record on database file.
     * Type passed must be a class derived from RecordBase
     * Must have a constructor without parameters or all of them must be optionals.
     * This class will make with some properties, marked with proper annotations
     * of RecordBaseAnnotation @see RecordBaseAnnotation annotation class.
     * These properties are primitives Kotlin types. But it is possible make
     * complex types using binary serialization.
     */
    inline fun <reified T : RecordBase> newRecord(obj: T) {
        newRecord(obj.readProperties())
    }

    /**
     * Create a new record on database file.
     * A record is a artificial name, doesn't exits anything particular on file.
     * This method pass some datums assigning the same record index.
     * @param datums : Datum objects to be write on data base.
     */
    fun newRecord(vararg datums: Datum) {
        newRecord(datums.toList())
    }

    /**
     * Create a new record on database file.
     * A record is a artificial name, doesn't exits anything particular on file.
     * This method pass some datums assigning the same record index.
     * @param datums : Datum objects to be write on data base.
     */
    fun newRecord(datums: List<Datum>) {
        var index = -1

        //Buscamos el índice de record mayor, iterando por todos los datums
        for (d in datums) {
            var indexRecord = indexRecordTable.firstOrNull { it.path == d.path }
            if (indexRecord != null) {
                if (index < indexRecord.record) {
                    index = indexRecord.record
                }
            }
        }

        //Incrementamos en uno el índice para el record
        index += 1

        //ASignamos a todos los datums el record y guardamos
        var fos = FileOutputStream(file, true)
        for (d in datums) {
            d.record = index
            write(fos, d)
        }
        fos.close()

        //Analizamos
        analyze()

    }

    /** @return a @see Cursor object to obtain data sequentially */
    inline fun <reified T : RecordBase> cursor(kclass: KClass<T>): Cursor<T> {
        return Cursor(this, kclass)
    }

    /**
     * Perform a search on data base
     * @return a list of custom data
     * If want to get all values must indicate 'true' as predicate.
     */
    inline fun <reified T : RecordBase> query(noinline predicate: ((T) -> Boolean)?): List<T> {

        //Resultado de los objetos T buscados
        val result = ArrayList<T>()

        //Flujo del archivo
        var fis: FileInputStream? = null

        //Objeto temporal
        var obj: T? = T::class.createInstance()

        //Paths que vamos a buscar
        var hashPaths = obj!!.hashPaths()

        try {
            //Abrimos el archivo
            fis = FileInputStream(file)

            //Iteramos cada datum disponible en el archivo
            while (fis.available() > 0) {

                //Obtenemos un datum
                read(fis)?.let { datum ->

                    if (datum.status == Status.NORMAL) {

                        //Evaluamos que el datum tenga un path que nos interese
                        if (hashPaths.contains(datum.pathHash)) {

                            //Buscamos si ya tenemos un objeto dentro de nuestra
                            //Lista de resultados con el record
                            //Si no lo agregamos
                            obj = result.find { it.record == datum.record }
                            if (obj == null) {
                                obj = T::class.createInstance()
                                obj!!.record = datum.record
                                obj!!.writeProperty(datum)
                                result.add(obj!!)
                            } else {
                                result[result.indexOf(obj!!)].writeProperty(datum)
                            }
                        }
                    }
                }
            }

            //Una vez que se han reconstruidos todos los objetos T
            //Aplicamos un sesgado partiendo del predicado
            //Para eliminar aquellos que no cumplan la condición
            if (predicate != null) {
                result.forEach {
                    if (!predicate(it)) {
                        result.remove(it)
                    }
                }
            }

        } catch (ex: Exception) {
            ex.printStackTrace()
        } finally {
            fis?.close()
        }
        return result
    }


    /**
     * Delete all "DELETE" marked datum
     */
    fun compact() {

        val file2 = File.createTempFile("flumen", "_temp")
        lateinit var fis: FileInputStream
        lateinit var fos: FileOutputStream

        try {
            //Open streams
            fis = FileInputStream(file)
            fos = FileOutputStream(file2)

            //Leemos cada datum en el origen
            while (fis.available() > 0) {
                read(fis)?.let { datum ->
                    if (datum.status != Status.DELETED) {
                        datum.status = Status.NORMAL
                    }
                    write(fos, datum)
                }
            }

            file.delete()
            file2.renameTo(file)

        } catch (ex: Exception) {
            ex.printStackTrace()
        } finally {
            fis.close()
            fos.close()
        }
    }


    /**
     * Read all datum objects. Filter by objs and write on a new database file
     *
     */

    fun <T : RecordBase> update(objs: List<T>) {
        //Create a temporary file
        val fileTmp = File.createTempFile("flumen", "update")

        try {
            val fosTmp = FileOutputStream(fileTmp)

            //Read each datum on each obj
            objs.forEach { obj ->
                obj.readProperties().forEach { datum ->
                    /*
                    If datums from obj are marked as update we change
                    datum status to NORMAL and then save it on
                    temporary file
                     */

                    if (datum.status == Status.UPDATE) {
                        datum.status = Status.NORMAL
                        //Write datum on temporary file
                        write(fosTmp, datum)
                    }

                    //Change status on main file
                    changeDatumStatus(datum.path, datum.record, Status.DELETED)
                }
            }

            fosTmp.flush()
            fosTmp.close()

            val fis = FileInputStream(fileTmp)
            val fos = FileOutputStream(file, true) //Main file

            //Append datums to main file (database file)
            val buffer = ByteArray(1024)
            while (fis.available() > 0) {
                fis.read(buffer)
                fos.write(buffer)
            }

            fos.flush()
            fis.close()
            fos.close()

        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            //Delete temporary file
            fileTmp.delete()
        }

    }
}



