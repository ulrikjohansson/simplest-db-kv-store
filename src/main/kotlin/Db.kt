package io.ulrik.db

import java.nio.file.Path
import kotlin.io.path.appendText
import kotlin.streams.toList

/*fun main() {
    throw NotImplementedError()
}*/

interface DbStore {
    fun get(key: Any): Any
    fun put(key: Any, value: Any): Unit
}

class ListStore : DbStore {
    private val store: MutableList<Pair<Any, Any>> = mutableListOf()
    override fun get(key: Any): Any {
        val values = store.stream().filter { it.first == key }.toList()
        return values.last().second
    }

    override fun put(key: Any, value: Any) {
        store.add(Pair(key, value))
    }
}

class FileStore(private val path: Path) : DbStore {

    override fun get(key: Any): Any {
        val matchingLines = mutableListOf<Any>()
        path.toFile().useLines {
            lines ->
            lines.forEach {
                if (it.startsWith("$key,")) {
                    matchingLines.add(
                        it.removePrefix("$key,")
                    )
                }
            }
        }
        return matchingLines.last()
    }

    override fun put(key: Any, value: Any) {
        path.appendText("$key,$value\n")
    }
}

class Db(private val store: DbStore = ListStore()) {

    fun put(key: String, value: String) {
        store.put(key, value)
    }

    fun get(key: String): Any {
        return store.get(key)
    }
}
