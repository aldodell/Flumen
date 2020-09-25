package org.philosophicas.flumen

class PayloadSizeOverflowException(var maxPayloadSize: Int) : Exception() {
    override val message: String?
        get() = "New payload size it is grater than old payload size."
            .plus("New payload size must be less than $maxPayloadSize bytes.")
}