package io.ulrik.db

import java.io.RandomAccessFile
import java.lang.IllegalStateException
import java.nio.file.Path
import kotlin.io.path.appendText
import kotlin.streams.toList

/*fun main() {
    throw NotImplementedError()
}*/

interface DbStore {
    fun get(key: Any): Any?
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

class FileStoreWithHashMap(private val path: Path) : DbStore {
    private val hashMap = mutableMapOf<Any, Long>()
    private val file = RandomAccessFile(path.toFile(), "rw")

    init {
        initHashmap()
    }

    private fun initHashmap() {
        file.seek(0)
        var line: String? = ""
        while (true) {
            val pos = file.filePointer
            line = file.readLine()
            if (line == null) {
                return
            }

            val splitLineList = line.split(',', limit = 2)
            if (splitLineList.size != 2) {
                throw IllegalStateException()
            }

            hashMap[splitLineList[0]] = pos
        }
    }

    override fun get(key: Any): Any? {
        val pos = hashMap[key] ?: return null
        file.seek(pos)
        val row = file.readLine()

        return row.removePrefix("$key,")
    }

    override fun put(key: Any, value: Any) {
        file.seek(file.length())
        val row = "$key,$value\n"
        hashMap[key] = file.filePointer
        file.writeBytes(row)
    }
}

class Db(private val store: DbStore = ListStore()) {

    fun put(key: String, value: String) {
        store.put(key, value)
    }

    fun get(key: String): Any? {
        return store.get(key)
    }
}
