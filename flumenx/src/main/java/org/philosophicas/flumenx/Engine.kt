package org.philosophicas.flumenx

import java.io.File


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

    /** Constuctor */
    constructor(file: File) {
        this.file = file
    }

    /** Constuctor */
    constructor(filename: String) {
        this.file = File(filename)
    }

}