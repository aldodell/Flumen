package org.philosophicas.flumenx

import java.nio.ByteBuffer

class Base64 {
    var base64chars: CharArray =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray()

    fun encode(byteArray: ByteArray): String {

        //Resultado
        var r = ""
        var mByteArray = byteArray
        var mod = mByteArray.size % 3

        //Módulo de la division anterior (residuo)
        for (i in 0..mod) {
            mByteArray += 0
        }

        var n: Int = 0
        var m = 0

        var buffer = ByteBuffer.wrap(mByteArray)

        for (i in 1..(mByteArray.size / 3)) {

            n = (buffer.get().toInt() and 255) shl 16
            n += (buffer.get().toInt() and 255) shl 8
            n += (buffer.get().toInt() and 255)

            m = (n shr 18) and 63
            r += base64chars[m]

            m = (n shr 12) and 63
            r += base64chars[m]

            m = (n shr 6) and 63
            r += base64chars[m]

            m = n and 63
            r += base64chars[m]


        }

        for (i in 0..mod) {
            r = r.removeSuffix("A")
        }

        for (i in 0..mod) {
            r += "="
        }

        return r
    }


    fun decode(value: String): ByteArray {

        var mValue = value

        var zeroes = 0
        var i = mValue.length - 1

        while (mValue[i] == "="[0]) {
            i -= 1
            zeroes += 1
            mValue = mValue.removeSuffix("=")
        }

        for (i in 1..zeroes) {
            mValue += "A"
        }


        var n = 0
        var m = 0
        var b = ArrayList<Byte>()

        for (i in 0..((mValue.length / 4)-1)) {

            n = base64chars.indexOf(mValue[4 * i]) shl 18
            n += base64chars.indexOf(mValue[4 * i + 1]) shl 12
            n += base64chars.indexOf(mValue[4 * i + 2]) shl 6
            n += base64chars.indexOf(mValue[4 * i + 3])

            m = (n shr 16) and 255
            b.add(m.toByte())

            m = (n shr 8) and 255
            b.add(m.toByte())

            m = n and 255
            b.add(m.toByte())
        }

        for (i in 1..zeroes) {
            b.removeAt(b.size - 1)
        }

        return b.toByteArray()
    }

}