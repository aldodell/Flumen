package org.philosophicas.flumen

import kotlin.reflect.KMutableProperty1
import kotlin.reflect.full.memberProperties


@Target(AnnotationTarget.PROPERTY, AnnotationTarget.FIELD, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class RecordBaseAnnotation(val path: String, val type: DatumType)

/**
 * Base class used like data adapter.
 */
open class RecordBase {
    /** Record field save an index like RowID to make relations between
     * datums
     */
    var record = -1

    /**
     * Denote entity status
     */
    var status: Status = Status.NORMAL

    fun paths(): Array<String> {
        val r = ArrayList<String>()

        val props = this::class.memberProperties
        for (p in props) {
            for (a in p.annotations) {
                if (a is RecordBaseAnnotation) {
                    if (!r.contains(a.path)) {
                        r.add(a.path)
                    }
                }
            }

        }
        return r.toTypedArray()
    }

    fun hashPaths(): Array<Int> {
        val r = ArrayList<Int>()

        val props = this::class.memberProperties
        for (p in props) {
            for (a in p.annotations) {
                if (a is RecordBaseAnnotation) {
                    if (!r.contains(a.path.hashCode())) {
                        r.add(a.path.hashCode())
                    }
                }
            }

        }
        return r.toTypedArray()
    }

    /**
     * Read object properties and @return a datum list
     */

    fun readProperties(): List<Datum> {
        var result = ArrayList<Datum>()
        var d = Datum("")
        val props = this::class.memberProperties
        for (p in props) {
            for (a in p.annotations) {
                val a2 = a as RecordBaseAnnotation
                when (a2.type) {
                    DatumType.String ->
                        d = Datum.from(p.getter.call(this) as? String ?: "", a2.path)

                    DatumType.Integer ->
                        d = Datum.from(p.getter.call(this) as? Int ?: 0, a2.path)

                    DatumType.Short ->
                        d = Datum.from(p.getter.call(this) as? Short ?: 0, a2.path)

                    DatumType.Long ->
                        d = Datum.from(p.getter.call(this) as? Long ?: 0, a2.path)

                    DatumType.Double ->
                        d = Datum.from(p.getter.call(this) as? Double ?: 0.0, a2.path)

                    DatumType.Float ->
                        d = Datum.from(p.getter.call(this) as? Float ?: 0F, a2.path)

                    DatumType.Boolean ->
                        d = Datum.from(p.getter.call(this) as? Boolean ?: false, a2.path)

                    else ->
                        d = Datum.from(p.getter.call(this) as? ByteArray ?: byteArrayOf(0), a2.path)

                }
                d.status = status
                result.add(d)
            }
        }
        return result
    }


    fun writeProperty(d: Datum) {
        val props = this::class.memberProperties
        for (p in props) {
            val a =
                p.annotations.find { (it as RecordBaseAnnotation).path == d.path } as RecordBaseAnnotation?
            if (a != null) {
                val p2 = p as KMutableProperty1
                when (a.type) {
                    DatumType.String ->
                        p2.setter.call(this, d.string)

                    DatumType.Integer ->
                        p2.setter.call(this, d.int)

                    DatumType.Short ->
                        p2.setter.call(this, d.short)

                    DatumType.Long ->
                        p2.setter.call(this, d.long)

                    DatumType.Double ->
                        p2.setter.call(this, d.double)

                    DatumType.Float ->
                        p2.setter.call(this, d.float)

                    DatumType.Boolean ->
                        p2.setter.call(this, d.boolean)

                    else ->
                        p2.setter.call(this, d.byteArray)
                }
                break
            }
        }
    }


    fun writeProperties(datums: List<Datum>) {
        val props = this::class.memberProperties
        for (d in datums) {
            for (p in props) {
                val a =
                    p.annotations.find { (it as RecordBaseAnnotation).path == d.path } as RecordBaseAnnotation?
                if (a != null) {
                    val p2 = p as KMutableProperty1
                    when (a.type) {
                        DatumType.String ->
                            p2.setter.call(this, d.string)

                        DatumType.Integer ->
                            p2.setter.call(this, d.int)

                        DatumType.Short ->
                            p2.setter.call(this, d.short)

                        DatumType.Long ->
                            p2.setter.call(this, d.long)

                        DatumType.Double ->
                            p2.setter.call(this, d.double)

                        DatumType.Float ->
                            p2.setter.call(this, d.float)

                        DatumType.Boolean ->
                            p2.setter.call(this, d.boolean)

                        else ->
                            p2.setter.call(this, d.byteArray)
                    }
                }
            }

        }

    }

    /** @return Datum object or null (if do not exits) by it's path
     *
     */
    fun datumBy(path: String): Datum? {
        val d: Datum
        val props = this::class.memberProperties
        for (p in props) {
            for (a in p.annotations) {
                val a2 = a as RecordBaseAnnotation
                when (a2.type) {
                    DatumType.String ->
                        d = Datum.from(p.getter.call(this) as? String ?: "", a2.path)

                    DatumType.Integer ->
                        d = Datum.from(p.getter.call(this) as? Int ?: 0, a2.path)

                    DatumType.Short ->
                        d = Datum.from(p.getter.call(this) as? Short ?: 0, a2.path)

                    DatumType.Long ->
                        d = Datum.from(p.getter.call(this) as? Long ?: 0, a2.path)

                    DatumType.Double ->
                        d = Datum.from(p.getter.call(this) as? Double ?: 0.0, a2.path)

                    DatumType.Float ->
                        d = Datum.from(p.getter.call(this) as? Float ?: 0F, a2.path)

                    DatumType.Boolean ->
                        d = Datum.from(p.getter.call(this) as? Boolean ?: false, a2.path)

                    else ->
                        d = Datum.from(p.getter.call(this) as? ByteArray ?: byteArrayOf(0), a2.path)

                }
                return d
            }
        }

        return null
    }

    fun remove() {
        status = Status.DELETED
    }

    fun update() {
        status = Status.UPDATE
    }

}