package io.ulrik.db

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.BufferedReader
import java.io.FileReader
import java.io.RandomAccessFile
import java.lang.IllegalStateException
import java.nio.file.Path
import java.util.Base64
import kotlin.io.path.appendText
import kotlin.io.path.createFile
import kotlin.io.path.exists
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.streams.toList
import kotlin.io.path.Path as KotlinPath

interface DbStore {
    fun get(key: String): Any?
    fun put(key: String, value: Any)
}

/**
 * In-memory store using a mutable list
 */
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

/**
 * A naive append-only file backend with no index.
 *
 * Filestore.get searches through the whole file from the beginning for all occurences of key,
 * and returns the last occurence.
 * Fixme: Just search backwards instead?
 * Fixme: Do we read the whole file into memory when searching? Can we not do that?
 */
class FileStore(private val path: Path) : DbStore {

    override fun get(key: String): Any {
        val matchingLines = mutableListOf<Any>()
        path.toFile().useLines { lines ->
            lines.forEach {
                val decodedLine = Base64.getDecoder().decode(it).decodeToString()
                if (decodedLine.startsWith("$key,")) {
                    matchingLines.add(
                        decodedLine.removePrefix("$key,")
                    )
                }
            }
        }
        return matchingLines.last()
    }

    override fun put(key: String, value: Any) {
        val encodedRow = Base64.getEncoder().encodeToString("$key,$value".toByteArray())
        path.appendText("$encodedRow\n")
    }
}

/**
 * A slightly less naive file backend using an index to keep track of the position (line) of the current
 * value for each key
 *
 * Stores each key/value entry as a base64 string to get around multi-byte character problems.
 * The index is a mutable hashmap in memory, and is serialized to a JSON file on disk.
 * This backend does not try to read the whole file into memory at any point,
 * so can be used with databases that do not fit into memory.
 * The index however, needs to fit in memory.
 *
 * This backend is MUCH faster (O(1)?) for gets on large db files, and still O(1) on writes
 */
class FileStoreWithHashMapIndex(private val path: Path, snapshotPath: Path = path) : DbStore {
    private val hashMapIndex = mutableMapOf<String, Long>()
    private val file = RandomAccessFile(path.toFile(), "rwd")
    private val reader = BufferedReader(FileReader(path.toFile()))
    private val indexFile = KotlinPath("$snapshotPath.idx")

    init {
        initHashmap()
    }

    private fun initHashmap() {

        if (indexFile.exists()) {
            val snapshotMap = Json.decodeFromString<Map<String, Long>>(indexFile.readText())
            hashMapIndex.clear()
            hashMapIndex.putAll(from = snapshotMap)
            return
        }

        var line: String?
        var pos: Long = 0
        while (true) {
            line = reader.readLine()
            if (line == null) {
                break
            }
            val decodedLine = Base64.getDecoder().decode(line).decodeToString()

            val splitLineList = decodedLine.split(',', limit = 2)
            if (splitLineList.size != 2) {
                throw IllegalStateException("The decoded line does not look like expected")
            }

            hashMapIndex[splitLineList[0]] = pos
            pos += line.length + 1
        }

        saveHashMapIndexToFile()
    }

    private fun saveHashMapIndexToFile() {
        if (!indexFile.exists()) {
            indexFile.createFile()
        }
        indexFile.writeText(Json.encodeToString(hashMapIndex))
    }

    override fun get(key: String): Any? {
        val pos = hashMapIndex[key] ?: return null
        file.seek(pos)
        val row = file.readLine()
        val decodedRow = Base64.getDecoder().decode(row).decodeToString()

        return decodedRow.removePrefix("$key,")
    }

    override fun put(key: String, value: Any) {
        file.seek(file.length())
        val row = "$key,$value"
        val encodedRow = Base64.getEncoder().encodeToString(row.toByteArray())
        hashMapIndex[key] = file.filePointer
        file.writeBytes("$encodedRow\n")
        saveHashMapIndexToFile()
    }
}

/**
 * DB frontend with a pluggable backend
 */
class Db(private val store: DbStore = ListStore()) {

    fun put(key: String, value: String) {
        store.put(key, value)
    }

    fun get(key: String): Any? {
        return store.get(key)
    }
}
