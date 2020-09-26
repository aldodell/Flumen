package org.philosophicas.flumenx

import kotlin.reflect.KMutableProperty1
import kotlin.reflect.full.createType
import kotlin.reflect.full.memberProperties


@Target(AnnotationTarget.PROPERTY)
annotation class Signature(val path: String, val type: Int)

/**
 * This class must be inherited to organize datums like a row on a data table.
 */

open class RecordTemplate(var path: String = "") {

    var record = 0

    fun writeProperties(datums: List<Datum>) {
        this::class.memberProperties.forEach { p ->
            p.annotations.forEach { a ->
                val an = a as? Signature
                datums.find { it.path == path + "/" + an?.path }?.let { datum ->
                    val p2 = p as KMutableProperty1
                    p2.setter.call(this, datum.int)

                    when (p2.returnType) {


                        Int::class.createType() -> {
                            p2.setter.call(this, datum.int)
                        }

                        Short::class.createType() -> {
                            p2.setter.call(this, datum.short)
                        }

                        Double::class.createType() -> {
                            p2.setter.call(this, datum.double)

                        }

                        Float::class.createType() -> {
                            p2.setter.call(this, datum.float)

                        }

                        String::class.createType() -> {
                            p2.setter.call(this, datum.string)

                        }

                        ByteArray::class.createType() -> {
                            p2.setter.call(this, datum.byteArray)

                        }

                        Boolean::class.createType() -> {
                            p2.setter.call(this, datum.boolean)

                        }
                    }
                }
            }
        }
    }

    fun readProperties(): List<Datum> {
        val result = ArrayList<Datum>()
        this::class.memberProperties.forEach { p ->
            p.annotations.forEach { a ->
                val an = a as? Signature
                val d = Datum()
                d.record = record
                d.path = path + "/" + an?.path

                when (p.returnType) {

                    Int::class.createType() -> {
                        d.int = p.getter.call(this) as Int
                    }

                    Short::class.createType() -> {
                        d.short = p.getter.call(this) as Short
                    }

                    Double::class.createType() -> {
                        d.double = p.getter.call(this) as Double
                    }

                    Float::class.createType() -> {
                        d.float = p.getter.call(this) as Float
                    }

                    String::class.createType() -> {
                        d.string = p.getter.call(this) as String
                    }

                    ByteArray::class.createType() -> {
                        d.byteArray = p.getter.call(this) as ByteArray
                    }

                    Boolean::class.createType() -> {
                        d.boolean = p.getter.call(this) as Boolean
                    }
                }
                result.add(d)
            }
        }
        return result.toList()
    }
}