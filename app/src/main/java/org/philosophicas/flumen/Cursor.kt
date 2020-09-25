package org.philosophicas.flumen

import java.io.FileInputStream
import kotlin.reflect.KClass
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.memberProperties


open class Cursor<T : RecordBase>(
    private val engine: Engine,
    private val kclass: KClass<T>
) : Iterator<T> {

    private var fis: FileInputStream = FileInputStream(engine.file)
    private val annotations = ArrayList<RecordBaseAnnotation>()
    private var recordIndex = 0

    init {
        val obj = kclass.createInstance()
        val props = obj::class.memberProperties
        for (prop in props) {
            if (prop.annotations.isNotEmpty()) {
                for (ann in prop.annotations) {
                    if (ann is RecordBaseAnnotation) {
                        annotations.add(ann)
                    }
                }
            }
        }
    }


    /**
     * Returns `true` if the iteration has more elements.
     */
    override fun hasNext(): Boolean {
        return fis.available() > 0
    }

    /**
     * Returns the next element in the iteration.
     */
    override fun next(): T {
        var datum: Datum?
        var datums = ArrayList<Datum>()
        var record = engine.indexRecordTable[recordIndex].record
        do {
            datum = engine.read(fis)
            if (datum != null) {
                if (annotations.any { it.path == datum.path }) {
                    if (datum.record == record) {
                        datums.add(datum)
                        if (datums.size == annotations.size) break
                    }
                }
            }
        } while (datum != null)

        recordIndex += 1
        val obj = kclass.createInstance()
        obj.writeProperties(datums)
        return obj
    }
}
