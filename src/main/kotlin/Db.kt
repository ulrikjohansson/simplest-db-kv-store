package io.ulrik.db

import kotlinx.serialization.*
import kotlinx.serialization.json.*
import java.io.BufferedReader
import java.io.FileReader
import java.io.RandomAccessFile
import java.lang.IllegalStateException
import java.nio.file.Path
import kotlin.io.path.*
import kotlin.streams.toList

/*fun main() {
    throw NotImplementedError()
}*/

interface DbStore {
    fun get(key: String): Any?
    fun put(key: String, value: Any)
}

class ListStore : DbStore {
    private val store: MutableList<Pair<Any, Any>> = mutableListOf()
    override fun get(key: String): Any {
        val values = store.stream().filter { it.first == key }.toList()
        return values.last().second
    }

    override fun put(key: String, value: Any) {
        store.add(Pair(key, value))
    }
}

class FileStore(private val path: Path) : DbStore {

    override fun get(key: String): Any {
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

    override fun put(key: String, value: Any) {
        path.appendText("$key,$value\n")
    }
}

class FileStoreWithHashMap(private val path: Path, snapshotPath: Path = path) : DbStore {
    private val hashMap = mutableMapOf<String, Long>()
    private val file = RandomAccessFile(path.toFile(), "rw")
    private val reader = BufferedReader(FileReader(path.toFile()))
    private val snapshotFile = Path("$snapshotPath.snapshot")

    init {
        initHashmap()
    }

    private fun initHashmap() {

        if (snapshotFile.exists()) {
            val snapshotMap = Json.decodeFromString<Map<String, Long>>(snapshotFile.readText())
            hashMap.clear()
            hashMap.putAll(from = snapshotMap)
            return
        }

        var line: String?
        var pos: Long = 0
        while (true) {
            line = reader.readLine()
            if (line == null) {
                break
            }

            val splitLineList = line.split(',', limit = 2)
            if (splitLineList.size != 2) {
                throw IllegalStateException()
            }

            hashMap[splitLineList[0]] = pos
            pos += line.length + 1
        }

        saveHashMapSnapshot()
    }

    private fun saveHashMapSnapshot() {
        if (!snapshotFile.exists()) { snapshotFile.createFile() }
        snapshotFile.writeText(Json.encodeToString(hashMap))
    }

    override fun get(key: String): Any? {
        val pos = hashMap[key] ?: return null
        file.seek(pos)
        val row = file.readLine()

        return row.removePrefix("$key,")
    }

    override fun put(key: String, value: Any) {
        file.seek(file.length())
        val row = "$key,$value\n"
        hashMap[key] = file.filePointer
        file.writeBytes(row)
        saveHashMapSnapshot()
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
